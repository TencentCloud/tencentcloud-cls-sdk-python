# -*- coding: utf-8 -*-

from enum import Enum


class ConsumerStatus(Enum):
    INITIALIZING = 'INITIALIZING'
    PROCESSING = 'PROCESSING'
    SHUTTING_DOWN = 'SHUTTING_DOWN'
    SHUTDOWN_COMPLETE = 'SHUTDOWN_COMPLETE'


class LogHubConfig(object):

    def __init__(self, endpoint, access_key_id, access_key, region, logset_id, topic_ids,
                 consumer_group_name, consumer_name, internal=False,
                 heartbeat_interval=None, data_fetch_interval=None, offset_position=None,
                 offset_start_time=None, security_token=None, max_fetch_log_group_size=None, worker_pool_size=None,
                 shared_executor=None,
                 offset_end_time=None):
        """
        :param endpoint:
        :param access_key_id:
        :param access_key:
        :param logset_id:
        :param topic_ids:
        :param consumer_group_name:
        :param internal: request go through the intranet or the internet.
        :param consumer_name: suggest use format "{consumer_group_name}-{current_process_id}", give it different consumer name when you need to run this program in parallel
        :param heartbeat_interval: default 20, once a client doesn't report to server * heartbeat_interval * 2 interval, server will consider it's offline and re-assign its task to another consumer. thus  don't set the heatbeat interval too small when the network badwidth or performance of consumtion is not so good.
        :param data_fetch_interval: default 2, don't configure it too small (<1s)
        :param offset_start_time: offset end time, could be "begin", "end", "specific time format in ISO", it's log receiving time.
        :param security_token:
        :param max_fetch_log_group_size: default 20000, fetch size in each request, normally use default. maximum is 20000, could be lower. the lower the size the memory efficiency might be better.
        :param worker_pool_size: default 2. suggest keep the default size (2), use multiple process instead, when you need to have more concurrent processing, launch this consumer for mulitple times and give them different consuer name in same consumer group. will be ignored when shared_executor is passed.
        :param shared_executor: shared executor, if not None, worker_pool_size will be ignored
        :param offset_end_time: offset end time, default is None (never stop processing). could setting it as ISO time-format, when setting it as "end", it means process all logs received from start to the time when the consumer is started.
        :param region: region of project
        """
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.access_key = access_key
        self.logset_id = logset_id
        self.topic_ids = topic_ids
        self.consumer_group_name = consumer_group_name
        self.consumer_name = consumer_name
        self.internal = internal
        self.heartbeat_interval = heartbeat_interval or 20
        self.data_fetch_interval = data_fetch_interval or 2
        self.offset_position = offset_position
        self.offset_start_time = offset_start_time or 'begin'  # default to begin
        self.securityToken = security_token
        self.max_fetch_log_group_size = max_fetch_log_group_size or 1000
        self.worker_pool_size = worker_pool_size or 2
        self.partition_executor = shared_executor
        self.consumer_group_time_out = self.heartbeat_interval * 2
        self.offset_end_time = offset_end_time or None  # default to None
        self.region = region
