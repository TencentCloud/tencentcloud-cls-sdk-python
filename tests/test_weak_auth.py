#!/usr/bin/env python

# Copyright (C) Tencent Cloud Computing
# All rights reserved.

"""弱鉴权（免密）上报单元测试。

使用标准库 unittest + 线程内桩 HTTP 服务端，不依赖网络与第三方测试框架。
"""

import json
import logging
import threading
import time
import unittest

from six.moves.BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

from tencentcloud.log.cls_pb2 import LogGroupList
from tencentcloud.log.error_code import (
    INVALID_UIN,
    MISS_ACCESS_KEY_ID,
    UNSUPPORTED_OPERATION,
)
from tencentcloud.log.logclient import (
    AUTH_MODE_WEAK,
    HEADER_AUTH_MODE,
    HEADER_UIN,
    LogClient,
    YunApiLogClient,
    _is_digits_only,
)
from tencentcloud.log.logexception import LogException

UIN = '100012345678'


def build_log_group_list():
    log_group_list = LogGroupList()
    log_group = log_group_list.logGroupList.add()
    log_group.filename = 'python.log'
    log_group.source = '127.0.0.1'
    log = log_group.logs.add()
    log.time = int(round(time.time() * 1000000))
    content = log.contents.add()
    content.key = 'Hello'
    content.value = 'World'
    return log_group_list


def _handle_post(handler):
    """处理 POST 请求：记录请求头，并按 server.stub_status 返回响应。"""
    length = int(handler.headers.get('Content-Length') or 0)
    if length:
        handler.rfile.read(length)

    handler.server.captured_headers = dict(handler.headers.items())
    handler.server.captured_path = handler.path

    body = handler.server.stub_body
    handler.send_response(handler.server.stub_status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', str(len(body)))
    handler.send_header('X-Cls-Requestid', handler.server.stub_request_id)
    handler.end_headers()
    handler.wfile.write(body)


class _StubHandler(BaseHTTPRequestHandler):
    """记录收到的请求头，并按 server.stub_status 返回响应。"""

    protocol_version = 'HTTP/1.1'

    def log_message(self, fmt, *args):
        pass


# 标准库按 'do_' + self.command 分发，self.command 为大写的 'POST'，
# 故处理方法名必须精确为 do_POST；在类体外赋值以保持方法名符合协议要求。
_StubHandler.do_POST = _handle_post


class StubServer:
    """线程内运行的桩 HTTP 服务端。"""

    def __init__(self, status=200, body=b'', request_id='stub-request-id'):
        self._httpd = HTTPServer(('127.0.0.1', 0), _StubHandler)
        self._httpd.stub_status = status
        self._httpd.stub_body = body
        self._httpd.stub_request_id = request_id
        self._httpd.captured_headers = {}
        self._httpd.captured_path = ''
        self._thread = threading.Thread(target=self._httpd.serve_forever)
        self._thread.daemon = True

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)

    @property
    def endpoint(self):
        host, port = self._httpd.server_address[:2]
        return f'{host}:{port}'

    def header(self, name):
        """按 HTTP 头名大小写不敏感的规则取值。"""
        for key, value in self._httpd.captured_headers.items():
            if key.lower() == name.lower():
                return value
        return None

    def has_header(self, name):
        return self.header(name) is not None


class _LogCollector(logging.Handler):
    """收集 logclient 模块产生的日志记录，兼容 Python 2.7（无 assertLogs）。"""

    def __init__(self):
        logging.Handler.__init__(self)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def messages(self, level=logging.WARNING):
        return [r.getMessage() for r in self.records if r.levelno >= level]

    def __enter__(self):
        logging.getLogger('tencentcloud.log.logclient').addHandler(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logging.getLogger('tencentcloud.log.logclient').removeHandler(self)


class TestIsDigitsOnly(unittest.TestCase):
    def test_accepts_ascii_digits(self):
        self.assertTrue(_is_digits_only('0'))
        self.assertTrue(_is_digits_only('100012345678'))

    def test_rejects_non_ascii_digits_and_bad_types(self):
        # str.isdigit() 会放过这些值（全角数字、上标、阿拉伯数字），这里必须拒绝
        for value in ['１２３', '²', '٣']:
            self.assertFalse(_is_digits_only(value), f'should reject {value!r}')
        for value in ['', ' 123', '123 ', '-1', '12a', 'abc', '1.0', None, 123, 1.5, [], {}]:
            self.assertFalse(_is_digits_only(value), f'should reject {value!r}')


class TestValidateCredentials(unittest.TestCase):
    """覆盖鉴权模式判定决策表（设计方案 §3.3）。"""

    endpoint = 'https://ap-guangzhou.cls.tencentcs.com'

    def _client(self, access_key_id, access_key, uin=None):
        return LogClient(self.endpoint, access_key_id, access_key, source='127.0.0.1', uin=uin)

    def test_full_ak_sk_without_uin_is_strong_auth(self):
        client = self._client('id', 'key')
        self.assertFalse(client._is_weak_auth())

    def test_full_ak_sk_with_uin_is_strong_auth_and_uin_ignored(self):
        client = self._client('id', 'key', uin=UIN)
        self.assertFalse(client._is_weak_auth())

    def test_only_uin_is_weak_auth(self):
        for access_key_id, access_key in [(None, None), ('', '')]:
            client = self._client(access_key_id, access_key, uin=UIN)
            self.assertTrue(client._is_weak_auth())

    def test_missing_credentials_raises_miss_access_key_id(self):
        with self.assertRaises(LogException) as ctx:
            self._client('', '')
        self.assertEqual(MISS_ACCESS_KEY_ID, ctx.exception.get_error_code())
        # 错误信息需提示 uin 也是一种选项
        self.assertIn('uin', ctx.exception.get_error_message())

    def test_partial_credentials_without_uin_raises_miss_access_key_id(self):
        # 半截密钥无法签名，必须归为弱鉴权候选（isWeakAuth 用 or 而非 and）
        for access_key_id, access_key in [('id', ''), ('', 'key')]:
            with self.assertRaises(LogException) as ctx:
                self._client(access_key_id, access_key)
            self.assertEqual(MISS_ACCESS_KEY_ID, ctx.exception.get_error_code())

    def test_partial_credentials_with_uin_is_weak_auth(self):
        client = self._client('id', '', uin=UIN)
        self.assertTrue(client._is_weak_auth())

    def test_non_digits_uin_raises_invalid_uin(self):
        for uin in ['abc', '-1', '1234abc', ' 123', '１２３']:
            with self.assertRaises(LogException) as ctx:
                self._client('', '', uin=uin)
            self.assertEqual(INVALID_UIN, ctx.exception.get_error_code(), f'uin={uin!r}')

    def test_weak_auth_over_http_logs_warning(self):
        secret_id = 'AKIDsecretidvalue'
        secret_key = 'secretkeyvalue'
        with _LogCollector() as collector:
            LogClient('http://ap-guangzhou.cls.tencentcs.com', secret_id, '',
                      source='127.0.0.1', uin=UIN)
        messages = collector.messages()
        self.assertEqual(1, len(messages))
        self.assertIn('plaintext', messages[0])
        # 告警日志不得包含任何密钥片段
        joined = '\n'.join(messages)
        for secret in (secret_id, secret_key):
            self.assertNotIn(secret, joined)
            self.assertNotIn(secret[:6], joined)

    def test_weak_auth_over_https_has_no_warning(self):
        with _LogCollector() as collector:
            LogClient('https://ap-guangzhou.cls.tencentcs.com', '', '', source='127.0.0.1', uin=UIN)
        self.assertEqual([], collector.messages())

    def test_strong_auth_construction_has_no_warning(self):
        with _LogCollector() as collector:
            LogClient('http://ap-guangzhou.cls.tencentcs.com', 'id', 'key', source='127.0.0.1')
        self.assertEqual([], collector.messages())

    def test_yunapi_client_never_uses_weak_auth(self):
        # 云 API 没有弱鉴权分支，即使不填密钥也不能被判为弱鉴权
        client = YunApiLogClient('', '', source='127.0.0.1')
        self.assertFalse(client._is_weak_auth())


class TestRequestHeaders(unittest.TestCase):
    """断言实际发出的请求头（设计方案 §2.1 / §7.1）。"""

    def test_weak_auth_sends_only_identity_headers(self):
        with StubServer() as server:
            client = LogClient(server.endpoint, '', '', source='127.0.0.1', uin=UIN)
            client.put_log_raw('topic-id', build_log_group_list())

            self.assertEqual(AUTH_MODE_WEAK, server.header(HEADER_AUTH_MODE))
            self.assertEqual(UIN, server.header(HEADER_UIN))
            # 核心语义：弱鉴权下绝不能带签名与临时密钥头
            self.assertFalse(server.has_header('Authorization'))
            self.assertFalse(server.has_header('X-Cls-Token'))
            # 数据格式相关的头不受影响
            self.assertEqual('application/x-protobuf', server.header('Content-Type'))
            self.assertEqual('lz4', server.header('x-cls-compress-type'))
            self.assertIn('log-python-sdk-v-', server.header('User-Agent'))

    def test_strong_auth_sends_signature_and_no_weak_headers(self):
        with StubServer() as server:
            client = LogClient(server.endpoint, 'id', 'key', securityToken='token', source='127.0.0.1')
            client.put_log_raw('topic-id', build_log_group_list())

            self.assertTrue(server.header('Authorization'))
            self.assertEqual('token', server.header('X-Cls-Token'))
            self.assertFalse(server.has_header(HEADER_AUTH_MODE))
            self.assertFalse(server.has_header(HEADER_UIN))

    def test_strong_auth_without_token_omits_token_header(self):
        with StubServer() as server:
            client = LogClient(server.endpoint, 'id', 'key', source='127.0.0.1')
            client.put_log_raw('topic-id', build_log_group_list())

            self.assertTrue(server.header('Authorization'))
            self.assertFalse(server.has_header('X-Cls-Token'))
            self.assertFalse(server.has_header(HEADER_AUTH_MODE))
            self.assertFalse(server.has_header(HEADER_UIN))

    def test_ak_sk_and_uin_coexist_sends_strong_auth(self):
        with StubServer() as server:
            client = LogClient(server.endpoint, 'id', 'key', source='127.0.0.1', uin=UIN)
            client.put_log_raw('topic-id', build_log_group_list())

            self.assertTrue(server.header('Authorization'))
            self.assertFalse(server.has_header(HEADER_AUTH_MODE))
            self.assertFalse(server.has_header(HEADER_UIN))


class TestErrorResponse(unittest.TestCase):
    def test_401_response_is_parsed_and_not_retried(self):
        body = json.dumps({
            'errorcode': 'AuthFailure.UnauthorizedOperation',
            'errormessage': 'topic topic-id anonymous access not enabled',
        }).encode('utf8')
        with StubServer(status=401, body=body, request_id='req-401') as server:
            client = LogClient(server.endpoint, '', '', source='127.0.0.1', uin=UIN)
            started = time.time()
            with self.assertRaises(LogException) as ctx:
                client.put_log_raw('topic-id', build_log_group_list())

        exception = ctx.exception
        # errorcode 原样透传，不做枚举映射
        self.assertEqual('AuthFailure.UnauthorizedOperation', exception.get_error_code())
        self.assertEqual('topic topic-id anonymous access not enabled', exception.get_error_message())
        self.assertEqual('req-401', exception.get_request_id())
        self.assertEqual(401, exception.resp_status)
        # 401 属配置错误，重试无意义，必须立即失败
        self.assertLess(time.time() - started, 1)


class TestWeakAuthUnsupportedOperations(unittest.TestCase):
    def test_pull_logs_rejected_under_weak_auth(self):
        client = LogClient('https://ap-guangzhou.cls.tencentcs.com', '', '', source='127.0.0.1', uin=UIN)
        with self.assertRaises(LogException) as ctx:
            client.pull_logs('topic-id', 0, 100)
        self.assertEqual(UNSUPPORTED_OPERATION, ctx.exception.get_error_code())


if __name__ == '__main__':
    unittest.main()
