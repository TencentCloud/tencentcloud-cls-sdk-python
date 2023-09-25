# -*- coding: utf-8 -*-

import json
import logging
import struct
import time
from copy import copy
from itertools import cycle

import requests
import six

from tencentcloud.log.auth import signature, signatureWithYunApiV3
from tencentcloud.log.consumer_group_request import *
from tencentcloud.log.consumer_group_response import *
from tencentcloud.log.logexception import LogException
from tencentcloud.log.pulllog_response import PullLogResponse
from tencentcloud.log.putlogsresponse import PutLogsResponse
from tencentcloud.log.util import Util
from tencentcloud.log.version import API_VERSION, USER_AGENT

logger = logging.getLogger(__name__)

if six.PY3:
    xrange = range

try:
    import lz4

    if not hasattr(lz4, 'loads') or not hasattr(lz4, 'dumps'):
        lz4 = None
    else:
        def lz_decompress(raw_size, data):
            return lz4.loads(struct.pack('<I', raw_size) + data)


        def lz_compresss(data):
            return lz4.dumps(data)[4:]

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


class LogClient(object):
    """ Construct the LogClient with endpoint, accessKeyId, accessKey.
    :type endpoint: string
    :param endpoint: log service host name, for example,  https://ap-guangzhou.cls.tencentcs.com
    :type accessKeyId: string
    :param accessKeyId: tencent cloud accessKeyId
    :type accessKey: string
    :param accessKey: tencent cloud accessKey
    """

    __version__ = API_VERSION
    Version = __version__

    def __init__(self, endpoint, accessKeyId, accessKey, securityToken=None, source=None, region='', is_https=False):
        self._isRowIp = Util.is_row_ip(endpoint)
        self._setendpoint(endpoint, is_https)
        self._accessKeyId = accessKeyId
        self._accessKey = accessKey
        self._timeout = CONNECTION_TIME_OUT
        if source is None:
            self._source = Util.get_host_ip(self._logHost)
        else:
            self._source = source
        self._securityToken = securityToken
        self._user_agent = USER_AGENT
        self._region = region

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

    def pull_logs(self, consumer_group, topic_id, partition_id, size, start_time=0, offset=0, end_time=None):
        """ batch pull log data from log service
        Unsuccessful operation will cause an LogException.

        :type consumer_group: string
        :param consumer_group: consumer group name

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
        :param start_time: the end time to get data

        :return: PullLogResponse

        :raise: LogException
        """

        body_dict = {
            'ConsumerGroup': consumer_group,
            'StartOffset': offset,
            'StartTime': start_time,
            'Size': size,
            'CompressType': 'snappy',
            'PartitionId': partition_id
        }
        if end_time is not None:
            body_dict['EndTime'] = end_time
            
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
    def __init__(self, accessKeyId, accessKey, internal=False, securityToken=None, source=None, region='',
                 is_https=True):
        yunapi_endpoint = CLS_YUNAPI_ENDPOINT
        if region != '':
            yunapi_endpoint = CLS_YUNAPI_ENDPOINT_TEMP % region
        if internal:
            yunapi_endpoint = CLS_YUNAPI_INTERNAL_ENDPOINT
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
