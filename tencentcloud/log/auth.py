#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Tencent Cloud Computing
# All rights reserved.

import hashlib
import hmac
import time

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
