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
    'protobuf>=3.4.0,<4.0.0',
    'lz4<=3.1.2',
    'python-dateutil',
    'python-snappy<=0.6.0'
]

requirements_py2 = [
    'six==1.14.0',
    'requests==2.23.0',
    'protobuf<=3.4.0',
    'lz4<=3.1.2',
    'python-dateutil',
    'python-snappy<=0.6.0'
]

requirements = []
if sys.version_info[0] == 2:
    requirements = requirements_py2
elif sys.version_info[0] == 3:
    requirements = requirements_py3

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
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: Implementation :: PyPy',
]

long_description = """
Python SDK for TencentCloud Log Service
http://tencentcloud-cls-sdk-python.readthedocs.io
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
