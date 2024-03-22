# -*- coding: utf-8 -*-


import json

import six

from tencentcloud.log.logresponse import LogResponse

__all__ = ['ConsumerGroupEntity',
           'ConsumerGroupGetOffsetsResponse',
           'ConsumerGroupHeartBeatResponse',
           'ConsumerGroupUpdateOffsetsResponse',
           'CreateConsumerGroupResponse',
           'DeleteConsumerGroupResponse',
           'ListConsumerGroupResponse',
           'UpdateConsumerGroupResponse']


class ConsumerGroupEntity(object):
    def __init__(self, consumer_group, timeout, topics):
        self.consumer_group_name = consumer_group
        self.timeout = timeout
        self.topics = topics

    def get_consumer_group_name(self):
        """

        :return:
        """
        return self.consumer_group_name

    def set_consumer_group_name(self, consumer_group_name):
        """

        :param consumer_group_name:
        :return:
        """
        self.consumer_group_name = consumer_group_name

    def get_timeout(self):
        """

        :return:
        """
        return self.timeout

    def set_timeout(self, timeout):
        """

        :param timeout:
        :return:
        """
        self.timeout = timeout

    def get_topics(self):
        """

        :return:
        """
        return self.topics

    def set_topics(self, topics):
        """

        :param topics:
        :return:
        """
        self.topics = topics

    def to_request_json(self):
        """

        :return:
        """
        log_store_dict = {
            'ConsumerGroup': self.get_consumer_group_name(),
            'Timeout': self.get_timeout(),
            'Topics': self.get_topics(),
        }
        return six.b(json.dumps(log_store_dict))

    def to_string(self):
        return "ConsumerGroup [ConsumerGroup=" + self.consumer_group_name \
            + ", Timeout=" + str(self.timeout) + ", Topics=" + ','.join(self.topics) + "]"


class CreateConsumerGroupResponse(LogResponse):
    def __init__(self, headers, resp=''):
        LogResponse.__init__(self, headers, resp['Response'])


class ConsumerGroupGetOffsetsResponse(LogResponse):
    def __init__(self, resp, headers, topic_id, partition_id):
        self.count = {}
        self.consumer_group_offsets = {}
        self.topic_id = topic_id
        self.partition_id = partition_id
        LogResponse.__init__(self, headers, resp)

        for topicPartitionOffsetsInfo in resp['Response']['TopicPartitionOffsetsInfo']:
            topic = topicPartitionOffsetsInfo['TopicID']
            partition_offsets = topicPartitionOffsetsInfo['PartitionOffsets']
            self.count[topic] = len(partition_offsets)
            self.consumer_group_offsets[topic] = partition_offsets

    def get_count(self):
        """

        :return:
        """

        return self.count

    def get_consumer_group_offsets(self):
        """

        :return:
        """
        return self.consumer_group_offsets

    def get_consumer_group_partition_offsets(self):
        for offset in self.consumer_group_offsets[self.topic_id]:
            if offset["PartitionId"] == self.partition_id:
                return offset["Offset"]
        return -1

    def log_print(self):
        """

        :return:
        """
        print('ListConsumerGroupOffsets:')
        print('headers:', self.get_all_headers())
        print('count:', self.count)
        print('consumer_group_offsets:', self.consumer_group_offsets)


class ConsumerGroupHeartBeatResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers, resp)
        self.partitions = resp['Response']['TopicPartitionsInfo']

    def get_partitions(self):
        """

        :return:
        """
        return self.partitions

    def set_partitions(self, partitions):
        """

        :param partitions:
        :return:
        """
        self.partitions = partitions

    def log_print(self):
        """

        :return:
        """
        print('ListHeartBeat:')
        print('headers:', self.get_all_headers())
        print('partitions:', self.partitions)


class ConsumerGroupUpdateOffsetsResponse(LogResponse):
    def __init__(self, headers, resp=''):
        LogResponse.__init__(self, headers, resp['Response'])


class DeleteConsumerGroupResponse(LogResponse):
    def __init__(self, headers, resp=''):
        LogResponse.__init__(self, headers, resp['Response'])

    def log_print(self):
        """
        :return:
        """
        print('DeleteConsumerGroupResponse:')
        print('headers:', self.get_all_headers())
        print('body:', self.get_body())


class ListConsumerGroupResponse(LogResponse):
    def __init__(self, resp, headers):
        LogResponse.__init__(self, headers, resp)
        self.consumer_groups = []
        consumer_groups_info = resp['Response']['ConsumerGroupsInfo']
        self._count = len(consumer_groups_info)
        for group_info in consumer_groups_info:
            self.consumer_groups.append(
                ConsumerGroupEntity(group_info['ConsumerGroup'], group_info['Timeout'], group_info['Topics']))

    def get_count(self):
        """

        :return:
        """
        return self._count

    def get_consumer_groups(self):
        """

        :return:
        """
        return self.consumer_groups

    def log_print(self):
        """

        :return:
        """
        print('ListConsumerGroupResponse:')
        print('headers:', self.get_all_headers())
        print('count:', self._count)
        for entity in self.consumer_groups:
            print(entity.to_string())


class UpdateConsumerGroupResponse(LogResponse):
    def __init__(self, headers, resp):
        LogResponse.__init__(self, headers, resp['Response'])
