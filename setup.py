#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (C) Tencent Cloud Computing
# All rights reserved.

"""Setup script for log service SDK.
You need to install google protocol buffer, setuptools and python-requests.
https://code.google.com/p/protobuf/
https://pypi.python.org/pypi/setuptools
http://docs.python-requests.org/
Depending on your version of Python, these libraries may also should be installed:
http://pypi.python.org/pypi/simplejson/
"""

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import re
import sys

requirements_py3 = [
    'six',
    'requests',
    'protobuf>=4.0.0',
    'lz4>=4.0.0',
    'python-dateutil',
    'python-snappy<=0.6.0'
]

requirements_py31 = [
    'six',
    'requests',
    'protobuf>=4.0.0',
    'lz4>=4.0.0',
    'python-dateutil',
    'python-snappy<=0.7.0'
]

requirements = []
major = sys.version_info[0]
minor = sys.version_info[1]
if major == 3:
    if minor < 10:
        requirements = requirements_py3
    else:
        requirements = requirements_py31

packages = [
    'tencentcloud',
    'tencentcloud.log',
    'tencentcloud/log/consumer'
]

version = ''
with open('tencentcloud/log/version.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)

classifiers = [
    'Development Status :: 5 - Production/Stable',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3.12',
    'Programming Language :: Python :: Implementation :: PyPy',
]

long_description = """
Python SDK for TencentCloud Log Service
"""

setup(
    name='tencentcloud-cls-sdk-python',
    version=version,
    description='TencentCloud cls log service Python client SDK',
    author='farmerx',
    url='https://github.com/TencentCloud/tencentcloud-cls-sdk-python',
    install_requires=requirements,
    packages=packages,
    classifiers=classifiers,
    long_description=long_description,
)
