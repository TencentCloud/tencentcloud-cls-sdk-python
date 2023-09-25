# -*- coding: utf-8 -*-

import json

import six

from tencentcloud.log.util import parse_timestamp

__all__ = ['CreateConsumerGroupRequest', 'ConsumerGroupGetOffsetsRequest',
           'ConsumerGroupHeartBeatRequest', 'ConsumerGroupUpdateOffsetsRequest']


class ConsumerGroupRequest(object):
    def __init__(self, logset_id):
        self.logset_id = logset_id

    def get_logset_id(self):
        return self.logset_id

    def set_logset_id(self, logset_id):
        self.logset_id = logset_id


class CreateConsumerGroupRequest(ConsumerGroupRequest):
    """ The request used to send create consumer group.

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: ConsumerGroupEntity
        :param consumer_group: consumer group info
        """

    def __init__(self, logset_id, consumer_group, timeout, topics):
        super(CreateConsumerGroupRequest, self).__init__(logset_id)
        self.consumer_group_name = consumer_group
        self.timeout = timeout
        self.topics = topics

    def get_request_body(self):
        body_dict = {
            "ConsumerGroup": self.consumer_group_name,
            "Timeout": self.timeout,
            "Topics": self.topics,
            "LogsetId": self.logset_id,
        }
        return six.b(json.dumps(body_dict))


class ConsumerGroupGetOffsetsRequest(ConsumerGroupRequest):
    """ The request used to send get offsets.

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type topic_id: string
        :param topic_id: topic id

        :type partition_id: int
        :param partition_id: partition id

        :type position: string
        :param position: position of get offsets, value is "start"、"end"、"unix timestamp" or readable time like "%Y-%m-%d %H:%M:%S<time_zone>"
        """

    def __init__(self, logset_id, consumer_group, topic_id, partition_id, position):
        super(ConsumerGroupGetOffsetsRequest, self).__init__(logset_id)
        self.consumer_group = consumer_group
        self.topic_id = topic_id
        self.partition_id = partition_id
        self.position = position

    def get_request_body(self):
        body_dict = {
            "ConsumerGroup": self.consumer_group,
            "TopicId": self.topic_id,
            "PartitionId": str(self.partition_id),
            "LogsetId": self.logset_id,
            "From": str(self.position) if self.position in ("begin", "end") else parse_timestamp(self.position)
        }
        return six.b(json.dumps(body_dict))


class ConsumerGroupHeartBeatRequest(ConsumerGroupRequest):
    """ The request used to send heart beat.

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type consumer: string
        :param consumer: consumer name

        :type partitions: list<partitionInfo>
        :param partitions: partitions info
        """

    def __init__(self, logset_id, consumer_group, consumer, partitions):
        super(ConsumerGroupHeartBeatRequest, self).__init__(logset_id)
        self.consumer_group = consumer_group
        self.consumer = consumer
        self.partitions = partitions

    def get_request_body(self):
        body_dict = {
            "ConsumerGroup": self.consumer_group,
            "Consumer": self.consumer,
            "LogsetId": self.logset_id,
            "TopicPartitionsInfo": self.partitions,
            "PartitionStrategy": 2
        }
        return six.b(json.dumps(body_dict))


class ConsumerGroupUpdateOffsetsRequest(ConsumerGroupRequest):
    """ The request used to send heart beat.

        :type logset_id: string
        :param logset_id: logset id

        :type consumer_group: string
        :param consumer_group: consumer group name

        :type consumer: string
        :param consumer: consumer name

        :type offsets: dict
        :param partitions: offsets info dict
        """

    def __init__(self, logset_id, consumer_group, consumer, offsets):
        super(ConsumerGroupUpdateOffsetsRequest, self).__init__(logset_id)
        self.consumer_group = consumer_group
        self.consumer = consumer
        self.offsets = offsets

    def get_consumer_group(self):
        return self.consumer_group

    def set_consumer_group(self, consumer_group):
        self.consumer_group = consumer_group

    def get_request_body(self):
        body_dict = {
            "TopicPartitionOffsetsInfo": self.offsets,
            "ConsumerGroup": self.consumer_group,
            "Consumer": self.consumer,
            "LogsetId": self.logset_id
        }
        return six.b(json.dumps(body_dict))
