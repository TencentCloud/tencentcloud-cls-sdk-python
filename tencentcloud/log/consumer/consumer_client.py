# -×- coding: utf-8 -*-

import logging

from tencentcloud.log.consumer.exceptions import ClientWorkerException
from tencentcloud.log.consumer.exceptions import OffsetException
from tencentcloud.log.logexception import LogException
from tencentcloud.log.version import USER_AGENT


class ConsumerClientLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        consumer_client = self.extra['consumer_client']  # type: ConsumerClient
        _id = '/'.join([
            consumer_client.logset_id, str(consumer_client.topic_ids),
            consumer_client.consumer_group, consumer_client.consumer
        ])
        return "[{0}] {1}".format(_id, msg), kwargs


class ConsumerClient(object):
    def __init__(self, endpoint, access_key_id, access_key, logset_id,
                 topic_ids, consumer_group, consumer, internal, region, security_token=None,
                 source=None):

        from tencentcloud.log.logclient import LogClient, YunApiLogClient

        self.client = LogClient(endpoint, access_key_id, access_key, security_token, source, region)
        self.yunapi_client = YunApiLogClient(access_key_id, access_key, internal, security_token,
                                             source, region)
        self.region = region
        self.client.set_user_agent('%s-consumer' % USER_AGENT)
        self.yunapi_client.set_user_agent('%s-consumer' % USER_AGENT)
        self.logset_id = logset_id
        self.topic_ids = topic_ids
        self.consumer_group = consumer_group
        self.consumer = consumer
        self.logger = ConsumerClientLoggerAdapter(
            logging.getLogger(__name__), {"consumer_client": self})

    def create_consumer_group(self, timeout):
        try:
            self.yunapi_client.create_consumer_group(self.logset_id, self.consumer_group,
                                                     timeout, self.topic_ids)
        except LogException as e:
            # consumer group already exist
            consumer_group_already_exist = 'consumer group already exists'
            if consumer_group_already_exist in e.get_error_message():
                try:
                    self.yunapi_client.update_consumer_group(self.logset_id, self.consumer_group,
                                                             self.topic_ids, timeout)

                except LogException as e1:
                    raise ClientWorkerException('error occour when update consumer group, errorCode: ' +
                                                e1.get_error_code() + ", errorMessage: " + e1.get_error_message())

            else:
                raise ClientWorkerException('error occour when create consumer group, errorCode: '
                                            + e.get_error_code() + ", errorMessage: " + e.get_error_message())

    def get_consumer_group(self):
        for consumer_group in \
                self.yunapi_client.list_consumer_group(self.logset_id, self.topic_ids,
                                                       self.region).get_consumer_groups():
            if consumer_group.get_consumer_group_name() == self.consumer_group:
                return consumer_group

        return None

    def heartbeat(self, partitions, response=None):
        if response is None:
            response = []

        try:
            response.extend(
                self.yunapi_client.heart_beat(self.logset_id, self.consumer_group,
                                              self.consumer, partitions).get_partitions())
            return True
        except LogException as e:
            self.logger.warning(e)

        return False

    def update_offsets(self, offsets):
        self.yunapi_client.update_offsets(self.logset_id, self.consumer_group,
                                          self.consumer, offsets)

    def get_offsets(self, topic_id='', partition_id=-1, position="end"):
        offsets = self.yunapi_client.get_offsets(self.logset_id, self.consumer_group, topic_id, self.region,
                                                 partition_id, position).get_consumer_group_offsets()

        if offsets is None or len(offsets) == 0:
            raise OffsetException('fail to get offsets')
        else:
            return offsets

    def get_partition_offsets(self, topic_id, partition_id, position="end"):
        if topic_id == '' or partition_id < 0:
            return -1

        return self.yunapi_client.get_offsets(self.logset_id, self.consumer_group, topic_id, partition_id,
                                              position).get_consumer_group_partition_offsets()

    def pull_logs(self, consumer_group, topic_id, partition_id, size, start_time=0, offset=0, end_time=None):
        return self.client.pull_logs(consumer_group, topic_id, partition_id, size, start_time, offset, end_time)

    def delete_consumer(self):
        return self.yunapi_client.delete_consumer_group(self.logset_id, self.consumer_group)
