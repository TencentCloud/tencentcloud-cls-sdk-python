# -*- coding: utf-8 -*-

import time
from multiprocessing import RLock

from tencentcloud.log.consumer.exceptions import OffsetException
from tencentcloud.log.logexception import LogException


class ConsumerOffsetTracker(object):
    def __init__(self, loghub_client_adapter, consumer_name, topic_id, partition_id):
        self.consumer_group_client = loghub_client_adapter
        self.consumer_name = consumer_name
        self.topic_id = topic_id
        self.partition_id = partition_id
        self.last_check_time = time.time()
        self.offset = ''
        self.temp_offset = ''
        self.last_persistent_offset = ''
        self.default_flush_offset_interval = 60
        self.lock = RLock()

    def set_offset(self, offset):
        self.offset = offset

    def get_offset(self):
        return self.offset

    def save_offset(self, persistent, offset=None):
        if offset is not None:
            self.temp_offset = offset
        else:
            self.temp_offset = self.offset
        if persistent:
            self.flush_offset()

    def set_memory_offset(self, offset):
        self.temp_offset = offset

    def set_persistent_offset(self, offset):
        self.last_persistent_offset = offset

    def flush_offset(self):
        with self.lock:
            if self.temp_offset != '' and self.temp_offset != self.last_persistent_offset:
                try:
                    self.consumer_group_client.update_offsets(self.generate_offsets(self.temp_offset))
                    self.last_persistent_offset = self.temp_offset
                except LogException as e:
                    raise OffsetException("Failed to persistent the offset to outside system, " +
                                          self.consumer_name + ", " + str(self.partition_id)
                                          + ", " + self.temp_offset, e)

    def flush_check(self):
        current_time = time.time()
        if current_time > self.last_check_time + self.default_flush_offset_interval:
            try:
                self.flush_offset()
            except OffsetException as e:
                print(e)
            self.last_check_time = current_time

    def generate_offsets(self, offset):
        topic_partition_offsets_info = []

        offset_info = dict()
        offset_info['TopicID'] = self.topic_id
        offset_info['PartitionOffsets'] = []

        offset_info_unit = dict()
        offset_info_unit["PartitionId"] = self.partition_id
        offset_info_unit["Offset"] = offset

        offset_info['PartitionOffsets'].append(offset_info_unit)
        topic_partition_offsets_info.append(offset_info)

        return topic_partition_offsets_info
