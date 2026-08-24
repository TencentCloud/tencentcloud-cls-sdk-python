#!/usr/bin/env python

# Copyright (C) Tencent Cloud Computing
# All rights reserved.

"""SDK local validation error codes.

Literals are kept consistent with the other language SDKs (for example
error_code.go in tencentcloud-cls-sdk-go), so that the server side and the
multi-language clients can be cross-checked by grep.
"""

# Missing secret. Also reported when uin is not provided under weak
# (secret-less) authorization, so existing user error-handling code keeps
# working.
MISS_ACCESS_KEY_ID = 'MissAccessKeyId'

# Invalid uin (not a digits-only string)
INVALID_UIN = 'InvalidUin'

# Operation not supported under weak (secret-less) authorization,
# for example the log consumption interfaces.
UNSUPPORTED_OPERATION = 'UnsupportedOperation'
