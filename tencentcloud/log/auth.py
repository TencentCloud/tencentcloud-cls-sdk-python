#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Tencent Cloud Computing
# All rights reserved.

import hashlib
import hmac
import sys
import time
from datetime import datetime

from six.moves.urllib.parse import quote_plus


def signature(access_key_id, access_key_secret, method='GET', path='/', params={}, headers={}, expire=120):
    filters_headers = dict((k, headers[k].encode('utf-8'))
                           for k in headers if k.lower() in ['content-type', 'content-md5', 'host'])

    format_str = u"{method}\n{path}\n{params}\n{headers}\n".format(
        method=method.lower(),
        path=path,
        params='&'.join(map(lambda tupl: "%s=%s" % (tupl[0].lower(), quote_plus(tupl[1])), sorted(params.items()))),
        headers='&'.join(
            map(lambda tupl: "%s=%s" % (tupl[0].lower(), quote_plus(tupl[1])), sorted(filters_headers.items())))
    )

    start_sign_time = int(time.time())
    sign_time = "{bg_time};{ed_time}".format(bg_time=start_sign_time - 60, ed_time=start_sign_time + expire)
    sha1 = hashlib.sha1()
    sha1.update(format_str.encode('utf-8'))

    str_to_sign = "sha1\n{time}\n{sha1}\n".format(time=sign_time, sha1=sha1.hexdigest())
    sign_key = hmac.new(access_key_secret.encode('utf-8'), sign_time.encode('utf-8'), hashlib.sha1).hexdigest()
    sign = hmac.new(sign_key.encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha1).hexdigest()
    sign_tpl = "q-sign-algorithm=sha1&q-ak={ak}&q-sign-time={sign_time}&q-key-time={key_time}&q-header-list={" \
               "headers}&q-url-param-list={params}&q-signature={sign} "
    return sign_tpl.format(
        ak=access_key_id,
        sign_time=sign_time,
        key_time=sign_time,
        params=';'.join(sorted(map(lambda k: k.lower(), params.keys()))),
        headers=';'.join(sorted(map(lambda k: k.lower(), filters_headers.keys()))),
        sign=sign
    )


def signatureWithYunApiV3(access_key_id, access_key_secret, service='cls',
                          method='GET', path='/', params={}, headers={}, data=''):
    canonical_uri = path
    canonical_querystring = ""
    payload = data

    if method == 'GET':
        canonical_querystring = params
        payload = ""

    if headers.get("X-TC-Content-SHA256") == "UNSIGNED-PAYLOAD":
        payload = "UNSIGNED-PAYLOAD"

    if sys.version_info[0] == 3 and isinstance(payload, type("")):
        payload = payload.encode("utf8")

    payload_hash = hashlib.sha256(payload).hexdigest()

    canonical_headers = 'content-type:%s\nhost:%s\n' % (
        headers["Content-Type"], headers["Host"])
    signed_headers = 'content-type;host'
    canonical_request = '%s\n%s\n%s\n%s\n%s\n%s' % (method,
                                                    canonical_uri,
                                                    canonical_querystring,
                                                    canonical_headers,
                                                    signed_headers,
                                                    payload_hash)

    algorithm = 'TC3-HMAC-SHA256'
    timestamp = int(time.time())
    if 'X-TC-Timestamp' in headers:
        timestamp = int(headers['X-TC-Timestamp'])
    date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
    credential_scope = date + '/' + service + '/tc3_request'
    if sys.version_info[0] == 3:
        canonical_request = canonical_request.encode("utf8")
    digest = hashlib.sha256(canonical_request).hexdigest()
    string2sign = '%s\n%s\n%s\n%s' % (algorithm,
                                      timestamp,
                                      credential_scope,
                                      digest)

    sign_str = sign_tc3(access_key_secret, date, service, string2sign)
    return "TC3-HMAC-SHA256 Credential=%s/%s/%s/tc3_request, SignedHeaders=content-type;host, Signature=%s" % (
        access_key_id, date, service, sign_str)


def sign_tc3(secret_key, date, service, str2sign):
    def _hmac_sha256(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256)

    def _get_signature_key(key, date, service):
        k_date = _hmac_sha256(('TC3' + key).encode('utf-8'), date)
        k_service = _hmac_sha256(k_date.digest(), service)
        k_signing = _hmac_sha256(k_service.digest(), 'tc3_request')
        return k_signing.digest()

    signing_key = _get_signature_key(secret_key, date, service)
    signature = _hmac_sha256(signing_key, str2sign).hexdigest()
    return signature
