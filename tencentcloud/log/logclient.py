import json
import struct
import logging
import requests
import time
import six
from copy import copy
from itertools import cycle

from tencentcloud.log.auth import signature
from tencentcloud.log.logexception import LogException
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

    def __init__(self, endpoint, accessKeyId, accessKey, securityToken=None, source=None, region=''):
        self._isRowIp = Util.is_row_ip(endpoint)
        self._setendpoint(endpoint)
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

    def _setendpoint(self, endpoint):
        self.http_type = 'http://'
        self._port = 80

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

    def _getHttpResponse(self, method, url, params, body, headers):  # ensure method, url, body is str
        try:
            headers['User-Agent'] = self._user_agent
            r = getattr(requests, method.lower())(url, params=params, data=body, headers=headers, timeout=self._timeout)
            return r.status_code, r.content, r.headers
        except Exception as ex:
            raise LogException('LogRequestError', str(ex))

    def _sendRequest(self, method, url, params, body, headers):
        (resp_status, resp_body, resp_header) = self._getHttpResponse(method, url, params, body, headers)
        header = {}
        for key, value in resp_header.items():
            header[key] = value

        requestId = Util.h_v_td(header, 'X-Cls-Requestid', '')
        if resp_status == 200:
            return resp_body, header

        exJson = self._loadJson(resp_status, resp_header, resp_body, requestId)
        exJson = Util.convert_unicode_to_str(exJson)

        if 'errorcode' in exJson and 'errormessage' in exJson:
            raise LogException(exJson['errorcode'], exJson['errormessage'], requestId,
                               resp_status, resp_header, resp_body)
        else:
            exJson = '. Return json is ' + str(exJson) if exJson else '.'
            raise LogException('LogRequestError',
                               'Request is failed. Http code is ' + str(resp_status) + exJson, requestId,
                               resp_status, resp_header, resp_body)

    def _send(self, method, body, resource, params, headers):
        url = self.http_type + self._endpoint + resource
        retry_times = range(10) if 'log-cli-v-' not in self._user_agent else cycle(range(10))
        last_err = None
        for _ in retry_times:
            try:
                headers2 = copy(headers)
                params2 = copy(params)
                if self._securityToken:
                    headers2["X-Cls-Token"] = self._securityToken

                authorization = signature(self._accessKeyId, self._accessKey, method, "/structuredlog", params2,
                                          headers2, 300)
                headers2["Authorization"] = authorization
                return self._sendRequest(method, url, params2, body, headers2)
            except LogException as ex:
                last_err = ex
                if ex.get_error_code() in ('InternalError', 'Timeout', 'SpeedQuotaExceed') or ex.resp_status >= 500 \
                        or (ex.get_error_code() == 'LogRequestError'
                            and 'httpconnectionpool' in ex.get_error_message().lower()):
                    time.sleep(1)
                    continue
                raise
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
        resource = '/structuredlog?topic_id=' + topic_id

        (resp, header) = self._send('POST', body, resource, params, headers)
        return PutLogsResponse(header, resp)

