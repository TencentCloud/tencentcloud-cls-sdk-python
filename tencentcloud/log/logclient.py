# -*- coding: utf-8 -*-

import json
import logging
import os
import struct
import time
from copy import copy
from itertools import cycle

import requests
import six

from tencentcloud.log.auth import signature, signatureWithYunApiV3
from tencentcloud.log.consumer_group_request import *
from tencentcloud.log.consumer_group_response import *
from tencentcloud.log.error_code import (
    INVALID_UIN,
    MISS_ACCESS_KEY_ID,
    UNSUPPORTED_OPERATION,
)
from tencentcloud.log.logexception import LogException
from tencentcloud.log.pulllog_response import PullLogResponse
from tencentcloud.log.putlogsresponse import PutLogsResponse
from tencentcloud.log.util import Util
from tencentcloud.log.version import API_VERSION, USER_AGENT

logger = logging.getLogger(__name__)

if six.PY3:
    xrange = range

try:
    try:
        import lz4.block as lz4
    except ImportError:
        import lz4

    if not hasattr(lz4, 'decompress') or not hasattr(lz4, 'compress'):
        lz4 = None
    else:
        def lz_decompress(raw_size, data):
            return lz4.decompress(struct.pack('<I', raw_size) + data)


        def lz_compresss(data):
            return lz4.compress(data)[4:]

except ImportError:
    lz4 = None

CONNECTION_TIME_OUT = 120
CONNECTION_TIME_OUT_YUNAPI = 30
RESPONSE_BODY_TYPE_BINARY = "binary"
RESPONSE_BODY_TYPE_JSON = "json"
CONSUMER_USER_AGENT = "tc-cls-sdk-python-consumer-v0.0.1"
API_VERSION = '2020-10-16'
CLS_YUNAPI_ENDPOINT = 'cls.tencentcloudapi.com'
CLS_YUNAPI_ENDPOINT_TEMP = 'cls.%s.tencentcloudapi.com'
CLS_YUNAPI_INTERNAL_ENDPOINT = 'cls.internal.tencentcloudapi.com'

# 弱鉴权（免密）相关请求头，字面量与服务端 consts.ClsAuthMode / consts.ClsUIN 保持一致
HEADER_AUTH_MODE = 'x-cls-auth-mode'
HEADER_UIN = 'X-CLS-Uin'
AUTH_MODE_WEAK = 'weak'

_DIGITS = '0123456789'


def _is_digits_only(value):
    """判断 value 是否为非空的纯 ASCII 数字字符串。

    不使用 str.isdigit()：它会放过全角数字（'１２３'）与 Unicode 上标（'²'）等字符，
    这些值发到服务端会被判为非法参数（HTTP 400），应在本地就拦住。
    """
    if not isinstance(value, six.string_types):
        return False
    if not value:
        return False
    for char in value:
        if char not in _DIGITS:
            return False
    return True


class LogClient(object):
    """ Construct the LogClient with endpoint, accessKeyId, accessKey.
    :type endpoint: string
    :param endpoint: log service host name, for example,  https://ap-guangzhou.cls.tencentcs.com
    :type accessKeyId: string
    :param accessKeyId: tencent cloud accessKeyId
    :type accessKey: string
    :param accessKey: tencent cloud accessKey
    :type uin: string
    :param uin: 弱鉴权（免密）账号 uin，与 accessKeyId/accessKey 二选一填写。
        两者同时填写时以 accessKeyId/accessKey 为准（走强鉴权），uin 被忽略。
        弱鉴权仅支持日志上传，日志消费仍必须使用云 API 密钥。
    """

    __version__ = API_VERSION
    Version = __version__

    # 是否支持弱鉴权（免密）。服务端弱鉴权只覆盖日志写入链路，
    # 云 API（管控）与日志消费接口没有对应分支，子类可置 False 关闭。
    _support_weak_auth = True

    # 旧 camelCase 关键字参数名 -> 新 snake_case 形参名，用于保持向后兼容
    _LEGACY_KWARGS = (
        ('accessKeyId', 'access_key_id'),
        ('accessKey', 'access_key'),
        ('securityToken', 'security_token'),
    )

    def __init__(self, endpoint, access_key_id=None, access_key=None, security_token=None, source=None, region='',
                 is_https=False, uin=None, **kwargs):
        # 兼容旧的 camelCase 关键字参数名（accessKeyId / accessKey / securityToken）
        current = {
            'access_key_id': access_key_id,
            'access_key': access_key,
            'security_token': security_token,
        }
        for legacy_name, new_name in self._LEGACY_KWARGS:
            if legacy_name in kwargs:
                legacy_value = kwargs.pop(legacy_name)
                if current[new_name] is None:
                    current[new_name] = legacy_value
        if kwargs:
            raise TypeError('unexpected keyword arguments: ' + ', '.join(sorted(kwargs)))
        access_key_id = current['access_key_id']
        access_key = current['access_key']
        security_token = current['security_token']

        self._isRowIp = Util.is_row_ip(endpoint)
        self._setendpoint(endpoint, is_https)
        self._accessKeyId = access_key_id
        self._accessKey = access_key
        self._uin = uin
        self._validate_credentials()
        self._timeout = CONNECTION_TIME_OUT
        if source is None:
            self._source = Util.get_host_ip(self._logHost)
        else:
            self._source = source
        self._securityToken = security_token
        self._user_agent = USER_AGENT
        self._region = region

    def _is_weak_auth(self):
        """ 判断当前是否走弱鉴权（免密）模式。

        仅当 accessKeyId/accessKey 不齐全时才是弱鉴权，即 AK/SK 优先。
        注意此处是 or 而非 and：半截密钥无法签名，只要不齐全就归为弱鉴权候选。

        :return: bool
        """
        if not self._support_weak_auth:
            return False
        return not self._accessKeyId or not self._accessKey

    def _validate_credentials(self):
        """ 构造期校验凭证配置，配置错误尽早暴露。

        AK/SK 齐全走强鉴权；否则要求填写合法的 uin 走弱鉴权（免密）。

        :raise: LogException
        """
        # 不支持弱鉴权的客户端（云 API）保持既有行为，不新增校验
        if not self._support_weak_auth:
            return

        if not self._is_weak_auth():
            return

        if not self._uin:
            raise LogException(MISS_ACCESS_KEY_ID,
                               'accessKeyId or accessKey cannot be empty, '
                               'or set uin to use weak authorization')
        if not _is_digits_only(self._uin):
            raise LogException(INVALID_UIN, 'uin must be a digits-only string')

        if self.http_type.lower() != 'https://':
            logger.warning('weak authorization transmits uin in plaintext over HTTP, '
                           'use an https endpoint or set is_https=True on untrusted networks')

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = value

    def set_user_agent(self, user_agent):
        """
        set user agent
        :type user_agent: string
        :param user_agent: user agent
        :return: None
        """
        self._user_agent = user_agent

    def _setendpoint(self, endpoint, is_https):
        self.http_type = 'http://'
        self._port = 80
        if is_https:
            self.http_type = 'https://'

        endpoint = endpoint.strip()
        pos = endpoint.find('://')
        if pos != -1:
            self.http_type = endpoint[:pos + 3]
            endpoint = endpoint[pos + 3:]

        if self.http_type.lower() == 'https://':
            self._port = 443

        pos = endpoint.find('/')
        if pos != -1:
            endpoint = endpoint[:pos]
        pos = endpoint.find(':')
        if pos != -1:
            self._port = int(endpoint[pos + 1:])
            endpoint = endpoint[:pos]
        self._logHost = endpoint
        self._endpoint = endpoint + ':' + str(self._port)

    @staticmethod
    def _loadJson(resp_status, resp_header, resp_body, requestId):
        if not resp_body:
            return None
        try:
            if isinstance(resp_body, six.binary_type):
                return json.loads(resp_body.decode('utf8', "ignore"))

            return json.loads(resp_body)
        except Exception as ex:
            raise LogException('BadResponse', 'Bad json format:\n' + repr(ex),
                               requestId, resp_status, resp_header, resp_body)

    @staticmethod
    def _error(json_body, resp_status, resp_header, resp_body, requestId):
        if 'errorcode' in json_body and 'errormessage' in json_body:
            raise LogException(json_body['errorcode'], json_body['errormessage'], requestId,
                               resp_status, resp_header, resp_body)
        elif 'Error' in json_body['Response'] and 'Code' in json_body['Response']['Error'] and 'Message' in \
                json_body['Response']['Error']:
            raise LogException(json_body['Response']['Error']['Code'], json_body['Response']['Error']['Message'],
                               requestId,
                               resp_status, resp_header, resp_body)
        else:
            exJson = '. Return json is ' + str(json_body) if json_body else '.'
            raise LogException('LogRequestError',
                               'Request is failed. Http code is ' + str(resp_status) + exJson, requestId,
                               resp_status, resp_header, resp_body)

    def _getHttpResponse(self, method, url, params, body, headers,
                         timeout=CONNECTION_TIME_OUT):  # ensure method, url, body is str
        try:
            headers['User-Agent'] = self._user_agent
            r = getattr(requests, method.lower())(url, params=params, data=body, headers=headers, timeout=timeout)
            return r.status_code, r.content, r.headers
        except Exception as ex:
            raise LogException('LogRequestError', str(ex))

    def _sendRequest(self, method, url, params, body, headers, response_body_type='json', timeout=CONNECTION_TIME_OUT):
        (resp_status, resp_body, resp_header) = self._getHttpResponse(method, url, params, body, headers,
                                                                      timeout=timeout)
        header = {}
        for key, value in resp_header.items():
            header[key] = value

        requestId = Util.h_v_td(header, 'X-Cls-Requestid', '')
        if resp_status == 200:
            if response_body_type == RESPONSE_BODY_TYPE_JSON:
                exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
                exJson = Util.convert_unicode_to_str(exJson)
                if 'Error' in exJson['Response']:
                    LogClient._error(exJson, resp_status, resp_header, resp_body, requestId)
                return exJson, header
            else:
                return resp_body, header

        exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
        exJson = Util.convert_unicode_to_str(exJson)

        LogClient._error(exJson, resp_status, resp_header, resp_body, requestId)

    def _send(self, method, body, resource, params, headers, response_body_type='json'):
        url = self.http_type + self._endpoint + resource
        retry_times = range(10) if 'log-cli-v-' not in self._user_agent else cycle(range(10))
        last_err = None
        for _ in retry_times:
            try:
                headers2 = copy(headers)
                params2 = copy(params)
                headers2['X-Qcloud-User-Id'] = os.getenv("HEADER_USER_ID", "")

                if self._is_weak_auth():
                    # 弱鉴权（免密）：只带明文身份头，不计算签名、不带 Authorization/X-Cls-Token
                    headers2[HEADER_AUTH_MODE] = AUTH_MODE_WEAK
                    headers2[HEADER_UIN] = self._uin
                else:
                    if self._securityToken:
                        headers2["X-Cls-Token"] = self._securityToken
                    authorization = signature(self._accessKeyId, self._accessKey, method, resource, params2,
                                              headers2, 300)
                    headers2["Authorization"] = authorization

                return self._sendRequest(method, url, params2, body, headers2, response_body_type)
            except LogException as ex:
                last_err = ex
                if ex.get_error_code() in ('InternalError', 'Timeout', 'SpeedQuotaExceed') or ex.resp_status >= 500 \
                        or (ex.get_error_code() == 'LogRequestError'
                            and 'httpconnectionpool' in ex.get_error_message().lower()):
                    time.sleep(1)
                    continue
                raise last_err
        raise last_err

    def put_log_raw(self, topic_id, log_group):
        """ Put logs to log service. using raw data in protobuf

        :type topic_id: string
        :param topic_id: the Project name

        :type log_group: LogGroup
        :param log_group: log group structure

        :return: PutLogsResponse
        :raise: LogException
        """

        body = log_group.SerializeToString()
        body = lz_compresss(body)
        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/x-protobuf',
            'x-cls-compress-type': 'lz4',
            'Content-Length': str(len(body))
        }
        params = {"topic_id": topic_id}
        resource = '/structuredlog'

        (resp, header) = self._send('POST', body, resource, params, headers, RESPONSE_BODY_TYPE_BINARY)
        return PutLogsResponse(header, resp)

    def pull_logs(self, topic_id, partition_id, size, start_time=0, offset=0, end_time=None, query=None):
        """ batch pull log data from log service
        Unsuccessful operation will cause an LogException.

        :type topic_id: string
        :param topic_id: topic id

        :type partition_id: int
        :param partition_id: partition id

        :type size: int
        :param size: the required data flow for pulling log packages

        :type offset: int
        :param offset: the offset position to get data

        :type start_time: int
        :param start_time: the start time to get data

        :type end_time: int
        :param end_time: the end time to get data

        :type query: string
        :param query: custom dsl filter rule

        :return: PullLogResponse

        :raise: LogException
        """

        # 服务端弱鉴权只覆盖日志写入链路，消费接口没有对应分支，提前给出可读报错
        if self._is_weak_auth():
            raise LogException(UNSUPPORTED_OPERATION,
                               'weak authorization does not support log consumption, '
                               'use accessKeyId/accessKey instead')

        body_dict = {
            'StartOffset': offset,
            'StartTime': int(start_time),
            'Size': size,
            'CompressType': 'snappy',
            'PartitionId': partition_id
        }

        if end_time is not None:
            body_dict['EndTime'] = int(end_time)

        if query:
            body_dict['Query'] = query

        body_str = six.b(json.dumps(body_dict))

        params = {'topic_id': topic_id}
        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/json',
        }

        resource = '/pull_log'
        (resp, header) = self._send("POST", body_str, resource, params, headers, RESPONSE_BODY_TYPE_BINARY)

        return PullLogResponse(resp, header)


class YunApiLogClient(LogClient):
    # 云 API（管控接口）没有弱鉴权分支，必须使用云 API 密钥
    _support_weak_auth = False

    def __init__(self, accessKeyId, accessKey, internal=False, securityToken=None, source=None, region='',
                 is_https=True):
        yunapi_endpoint = CLS_YUNAPI_ENDPOINT
        if region != '':
            yunapi_endpoint = CLS_YUNAPI_ENDPOINT_TEMP % region
        if internal:
            yunapi_endpoint = CLS_YUNAPI_INTERNAL_ENDPOINT
        yunapi_endpoint = os.getenv("YUNAPI_ENDPOINT", yunapi_endpoint)
        super(YunApiLogClient, self).__init__(yunapi_endpoint, accessKeyId, accessKey, securityToken, source, region,
                                              is_https)

    def _send(self, method, resource, params, headers, body='', region='', action='',
              response_body_type='json', service='cls'):
        url = self.http_type + self._endpoint + resource
        retry_times = range(10) if 'log-cli-v-' not in self._user_agent else cycle(range(10))
        last_err = None
        timestamp = int(time.time())
        for _ in retry_times:
            try:
                headers2 = copy(headers)
                params2 = copy(params)
                headers2['X-TC-Timestamp'] = str(timestamp)
                headers2['X-TC-Language'] = 'zh-CN'
                headers2['X-TC-Action'] = action
                headers2['X-TC-Region'] = region
                headers2['X-Qcloud-User-Id'] = os.getenv("HEADER_USER_ID", "")
                if self._securityToken:
                    headers2["X-Cls-Token"] = self._securityToken

                authorization = signatureWithYunApiV3(self._accessKeyId, self._accessKey, service,
                                                      method, resource, params2, headers2, body)
                headers2["Authorization"] = authorization
                return self._sendRequest(method, url, params2, body, headers2, response_body_type,
                                         timeout=CONNECTION_TIME_OUT_YUNAPI)
            except LogException as ex:
                last_err = ex
                if ex.get_error_code() in ('InternalError', 'Timeout', 'SpeedQuotaExceed') or ex.resp_status >= 500 \
                        or (ex.get_error_code() == 'LogRequestError'
                            and 'httpconnectionpool' in ex.get_error_message().lower()):
                    time.sleep(1)
                    continue
                raise last_err
        raise last_err

    def create_consumer_group(self, logset_id, consumer_group, timeout, topics):
        """ create consumer group

        :type logset_id: string
        :param logset_id: logset_id

        :type topics: list
        :param topics: list of topic_id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type timeout: int
        :param timeout: time-out in second

        :return: CreateConsumerGroupResponse
        """
        request = CreateConsumerGroupRequest(logset_id, consumer_group, timeout, topics)
        body_str = request.get_request_body()

        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/json',
            'X-TC-Version': API_VERSION
        }
        params = {}

        resource = '/'
        (resp, header) = self._send('POST', resource, params, headers, body_str, self._region, 'CreateConsumerGroup')
        return CreateConsumerGroupResponse(header, resp)

    def update_consumer_group(self, logset_id, consumer_group, topics, timeout=None):
        """ Update consumer group

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type timeout: int
        :param timeout: timeout

        :type topics: list
        :param topics: topic list

        :return: UpdateConsumerGroupResponse
        """
        if timeout is None:
            raise ValueError('timeout can\'t all be None')
        elif topics is not None and timeout is not None:
            body_dict = {
                'Topics': topics,
                'Timeout': timeout
            }
        elif topics is not None:
            body_dict = {
                'Topics': topics
            }
        else:
            body_dict = {
                'timeout': timeout
            }
        body_dict['LogsetId'] = logset_id
        body_dict['ConsumerGroup'] = consumer_group
        body_str = six.b(json.dumps(body_dict))

        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/json',
            'X-TC-Version': API_VERSION
        }
        params = {}
        resource = '/'
        (resp, header) = self._send('POST', resource, params, headers, body_str, self._region, 'ModifyConsumerGroup')
        return UpdateConsumerGroupResponse(header, resp)

    def delete_consumer_group(self, logset_id, consumer_group):
        """ Delete consumer group

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :return: DeleteConsumerGroupResponse
        """

        body_dict = {
            'LogsetId': logset_id,
            'ConsumerGroup': consumer_group
        }
        body_str = six.b(json.dumps(body_dict))

        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/json',
            'X-TC-Version': API_VERSION
        }

        params = {}

        resource = '/'
        (resp, header) = self._send('POST', resource, params, headers, body_str, self._region, 'DeleteConsumerGroup')
        return DeleteConsumerGroupResponse(header, resp)

    def list_consumer_group(self, logset_id, topics):
        """ List consumer group

        :type logset_id: string
        :param logset_id: logset id

        :type topics: list
        :param topics: topic id list

        :return: ListConsumerGroupResponse
        """

        body_dict = {
            'LogsetId': logset_id,
            'Topics': topics
        }
        body_str = six.b(json.dumps(body_dict))

        resource = '/'
        params = {}
        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/json',
            'X-TC-Version': API_VERSION
        }

        (resp, header) = self._send('POST', resource, params, headers, body_str, self._region, 'DescribeConsumerGroups')
        return ListConsumerGroupResponse(resp, header)

    def update_offsets(self, logset_id, consumer_group, consumer='', offsets=None):
        """ Update check point

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type consumer: string
        :param consumer: consumer name

        :type offsets: dict
        :param offsets: offset info dict

        :return: ConsumerGroupUpdateOffsetsResponse
        """
        request = ConsumerGroupUpdateOffsetsRequest(logset_id, consumer_group, consumer, offsets)
        body_str = request.get_request_body()
        params = {}
        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/json',
            'X-TC-Version': API_VERSION
        }

        resource = '/'
        (resp, header) = self._send("POST", resource, params, headers, body_str, self._region, 'CommitConsumerOffsets')
        return ConsumerGroupUpdateOffsetsResponse(header, resp)

    def get_offsets(self, logset_id, consumer_group, topic_id, partition_id=-1, position="end"):
        """ Get offsets

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type topic_id: string
        :param topic_id: topic id

        :type partition_id: int
        :param partition_id: partition id

        :type position: string
        :param position: position of get offsets, value is "start"、"end"、"unix timestamp" or readable time like "%Y-%m-%d %H:%M:%S<time_zone>"

        :return: ConsumerGroupGetOffsetsResponse
        """
        request = ConsumerGroupGetOffsetsRequest(logset_id, consumer_group, topic_id, partition_id, position)
        body_str = request.get_request_body()
        params = {}
        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/json',
            'X-TC-Version': API_VERSION
        }

        resource = '/'
        (resp, header) = self._send("POST", resource, params, headers, body_str, self._region,
                                    'DescribeConsumerOffsets')
        return ConsumerGroupGetOffsetsResponse(resp, header, topic_id, partition_id)

    def heart_beat(self, logset_id, consumer_group, consumer='', partitions=None):
        """ Update check point

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type consumer: string
        :param consumer: consumer name

        :type partitions: list
        :param partitions: partition info list

        :return: ConsumerGroupHeartBeatResponse
        """
        if partitions is None:
            partitions = []
        request = ConsumerGroupHeartBeatRequest(logset_id, consumer_group, consumer, partitions)
        body_str = request.get_request_body()
        params = {}
        headers = {
            'Host': self._logHost,
            'Content-Type': 'application/json',
            'X-TC-Version': API_VERSION
        }

        resource = '/'
        (resp, header) = self._send("POST", resource, params, headers, body_str, self._region, 'SendConsumerHeartbeat')
        return ConsumerGroupHeartBeatResponse(resp, header)
