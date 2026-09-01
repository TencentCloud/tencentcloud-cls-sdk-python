#!/usr/bin/env python

# Copyright (C) Tencent Cloud Computing
# All rights reserved.

"""版本号与 User-Agent 组装测试。

锁住版本号格式与 UA 前缀，避免发版时只打 tag 不改常量导致 UA 上报的版本失真。
"""

import os
import re
import unittest

from tencentcloud.log.logclient import LogClient
from tencentcloud.log.version import USER_AGENT, __version__

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestVersion(unittest.TestCase):
    def test_version_format(self):
        self.assertTrue(re.match(r'^\d+\.\d+\.\d+$', __version__),
                        f'unexpected version format: {__version__}')

    def test_user_agent_format(self):
        self.assertTrue(USER_AGENT.startswith('log-python-sdk-v-' + __version__))

    def test_setup_py_reads_same_version(self):
        # setup.py 用正则从 version.py 提取版本号，确保两者不会脱节
        with open(os.path.join(ROOT, 'tencentcloud', 'log', 'version.py')) as fd:
            parsed = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)
        self.assertEqual(__version__, parsed)

    def test_custom_user_agent(self):
        client = LogClient('https://ap-guangzhou.cls.tencentcs.com', 'id', 'key', source='127.0.0.1')
        client.set_user_agent('my-agent')
        self.assertEqual('my-agent', client._user_agent)


if __name__ == '__main__':
    unittest.main()
