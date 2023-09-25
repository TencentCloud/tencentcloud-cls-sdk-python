#!/usr/bin/env python
# encoding: utf-8
import base64
import json
import struct

import six

from tencentcloud.log.cls_pb2 import LogGroup
from tencentcloud.log.logexception import LogException
from tencentcloud.log.logresponse import LogResponse

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

DEFAULT_DECODE_LIST = ('utf8',)
VERSION = 130
MSGHEADERLEN = 96


class PullLogResponse(LogResponse):
    """ The response of the pull_logs API from log.
    :type resp: dict
    :param resp: the HTTP response body
    """

    def __init__(self, resp, header):
        LogResponse.__init__(self, header, resp)
        self.resp = self.decompress_resp(resp)
        self.next_offset = self.resp['Response']["NextOffset"]
        self.log_groups = []
        self._parse_log_groups(self.resp['Response']["Message"])
        self.flatten_logs_json = []

    def decompress_resp(self, resp):
        import snappy

        decompress = snappy.decompress(resp)
        try:
            if isinstance(decompress, six.binary_type):
                return json.loads(decompress.decode('utf8', "ignore"))

            return json.loads(decompress)
        except Exception as ex:
            raise LogException('BadResponse', 'Bad json format:\n' + repr(ex),
                               self.get_request_id(), self.get_all_headers(), decompress)

    def get_body(self):
        if self._body is None:
            self._body = {"next_offset": self.next_offset,
                          "count": len(self.get_flatten_logs_json()),
                          "logs": self.get_flatten_logs_json()}
        return self._body

    @property
    def body(self):
        return self.get_body()

    @body.setter
    def body(self, value):
        self._body = value

    def get_next_offset(self):
        return self.next_offset

    def get_log_count(self):
        return len(self.get_flatten_logs_json())

    def get_log_group_count(self):
        return len(self.log_groups)

    def get_log_group_json_list(self):
        if self.log_groups_json is None:
            self._transfer_to_json()
        return self.log_groups_json

    def get_log_groups(self):
        return self.log_groups

    def log_print(self):
        print('PullLogResponse')
        print('next_offset', self.next_offset)
        print('log_count', len(self.get_flatten_logs_json()))
        print('headers:', self.get_all_headers())
        print('detail:', self.get_log_group_json_list())

    def _parse_log_groups(self, data):
        try:
            if data is not None:
                for message in data:
                    b_message = base64.b64decode(message)
                    log_group_binary = Message(b_message)
                    log_group_binary.parse_message()
                    log_group = LogGroup()
                    log_group.ParseFromString(log_group_binary.data)
                    self.log_groups.append(log_group)
        except Exception as ex:
            err = 'failed to parse data to LogGroup: \n' + str(ex)
            raise LogException('BadResponse', err)

    def _transfer_to_json(self):
        self.log_groups_json = []
        for log_group in self.log_groups:
            items = []
            tags = {}
            for tag in log_group.logTags:
                tags[tag.key] = tag.value

            for log in log_group.logs:
                item = {'@lh_time': log.time}
                for content in log.contents:
                    item[content.key] = content.value
                items.append(item)
            log_items = {'filename': log_group.filename, 'source': log_group.source,
                         'logs': items,
                         'tags': tags}
            self.log_groups_json.append(log_items)

    @staticmethod
    def get_log_count_from_group(log_groups):
        count = 0
        for log_group in log_groups:
            for log in log_group.Logs:
                count += 1
        return count

    @staticmethod
    def log_groups_to_flattern_list(log_groups, time_as_str=None):
        flatten_logs_json = []
        for log_group in log_groups:
            tags = {}
            for tag in log_group.logTags:
                tags[u"__tag__:{0}".format(tag.key)] = tag.value

            for log in log_group.logs:
                item = {u'__timestamp__': six.text_type(log.time) if time_as_str else log.time,
                        u'__filename__': log_group.filename,
                        u'__source__': log_group.source}
                item.update(tags)
                for content in log.contents:
                    item[content.key] = content.value
                flatten_logs_json.append(item)
        return flatten_logs_json

    def get_flatten_logs_json(self, time_as_str=None):
        if self.flatten_logs_json is None:
            self.flatten_logs_json = self.log_groups_to_flattern_list(self.log_groups, time_as_str=time_as_str)

        return self.flatten_logs_json


class Message(object):
    def __init__(self, src):
        self.src = src
        self.version = -1
        self.timestamp = -1
        self.uin = -1
        self.logset_id = ''
        self.topic_id = ''
        self.content_len = -1
        self.data = ''

    def parse_message(self):
        if len(self.src) <= MSGHEADERLEN:
            raise LogException('InvalidLogGroup', 'log group list is too short to parse')

        self.version = struct.unpack('>i', self.src[:4])[0]
        if self.version == VERSION:
            self.timestamp, self.uin, self.logset_id, self.topic_id, self.content_len = \
                struct.unpack('>qq36s36si', self.src[4:MSGHEADERLEN])
            if self.content_len != len(self.src[MSGHEADERLEN:]):
                raise LogException('InvalidLogGroup',
                                   'declared log group content length is not equal to actual message length')

            self.data = struct.unpack('>{}s'.format(self.content_len), self.src[MSGHEADERLEN:])[0]
        else:
            err_msg = 'log group list version is not supported, version: {}'.format(self.version)
            raise LogException('InvalidLogGroupVersion', err_msg)
