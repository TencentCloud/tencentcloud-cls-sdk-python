# -*- coding: utf-8 -×-


class FetchedLogGroup(object):

    def __init__(self, partition_id, log_groups, end_offset):
        self._partition_id = partition_id
        self._fetched_log_groups = log_groups
        self._end_offset = end_offset

    @property
    def partition_id(self):
        return self._partition_id

    @property
    def fetched_log_groups(self):
        return self._fetched_log_groups

    @property
    def end_offset(self):
        return self._end_offset

    @property
    def log_group_size(self):
        return len(self._fetched_log_groups)
