# -*- coding: utf-8 -*-

import copy
import logging
import time
from multiprocessing import RLock
from threading import Thread


class HeartBeatLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        heart_beat = self.extra['heart_beat']  # type: ConsumerHeatBeat
        _id = '/'.join([
            heart_beat.log_client.logset_id, str(heart_beat.log_client.topic_ids),
            heart_beat.log_client.consumer_group,
            heart_beat.log_client.consumer
        ])
        return "[{0}] {1}".format(_id, msg), kwargs


class ConsumerHeatBeat(Thread):

    def __init__(self, log_client, topic_ids, heartbeat_interval, consumer_group_time_out):
        super(ConsumerHeatBeat, self).__init__()
        self.log_client = log_client
        self.heartbeat_interval = heartbeat_interval
        self.mheld_partitions = []
        self.mheart_partitions = []
        for topic_id in topic_ids:
            self.mheld_partitions.append({"TopicID": topic_id, "Partitions": []})
            self.mheart_partitions.append({"TopicID": topic_id, "Partitions": []})
        self.shut_down_flag = False
        self.lock = RLock()
        self.last_hearbeat_successed_unixtime = time.time()
        self.consumer_group_time_out = consumer_group_time_out
        self.logger = HeartBeatLoggerAdapter(
            logging.getLogger(__name__), {"heart_beat": self})

    def run(self):
        self.logger.info('heart beat start')
        while not self.shut_down_flag:
            try:
                response_partitions = []
                last_heatbeat_time = time.time()

                if self.log_client.heartbeat(self.mheart_partitions, response_partitions):
                    self.last_hearbeat_successed_unixtime = time.time()
                    self.logger.debug('heart beat result: %s get: %s',
                                      self.mheart_partitions, response_partitions)

                    ConsumerHeatBeat._sort(self.mheart_partitions)
                    ConsumerHeatBeat._sort(response_partitions)
                    if not ConsumerHeatBeat._equal(self.mheart_partitions, response_partitions):
                        add_set = ConsumerHeatBeat._sub(response_partitions, self.mheart_partitions)
                        remove_set = ConsumerHeatBeat._sub(self.mheart_partitions, response_partitions)
                        if any([add_set, remove_set]):
                            self.logger.info(
                                "partition reorganize, adding: %s, removing: %s",
                                add_set, remove_set)
                else:
                    if time.time() - self.last_hearbeat_successed_unixtime > \
                            (self.consumer_group_time_out + self.heartbeat_interval):
                        response_partitions = []
                        self.logger.info(
                            "Heart beat timeout, automatic reset consumer held partitions")
                    else:
                        with self.lock:
                            response_partitions = self.mheld_partitions
                            self.logger.info(
                                "Heart beat failed, Keep the held partitions unchanged")

                with self.lock:
                    self.mheart_partitions = ConsumerHeatBeat._add(copy.deepcopy(self.mheart_partitions),
                                                                   copy.deepcopy(response_partitions))
                    self.mheld_partitions = copy.deepcopy(response_partitions)

                # default sleep for 2s from "LogHubConfig"
                time_to_sleep = self.heartbeat_interval - (time.time() - last_heatbeat_time)
                while time_to_sleep > 0 and not self.shut_down_flag:
                    time.sleep(min(time_to_sleep, 1))
                    time_to_sleep = self.heartbeat_interval - (time.time() - last_heatbeat_time)
            except Exception as e:
                self.logger.warning("fail to heart beat", e)

        self.logger.info('heart beat exit')

    def get_held_partitions(self):
        """
        must copy to prevent race condition in multi-threads
        data format:[{"TopicId":xxx, "Partitions":[xx,xx,xx]}]
        :return:
        """
        return self.mheld_partitions[:]

    def shutdown(self):
        self.logger.info('try to stop heart beat')
        self.shut_down_flag = True

    def remove_heart_partition(self, topic_id, partition_id):
        self.logger.info('try to remove partition "{0}:{1}", current partitons: {2}'.format(topic_id, partition_id,
                                                                                            self.mheld_partitions))
        with self.lock:
            try:
                for part in self.mheld_partitions:
                    if part['TopicID'] == topic_id:
                        part['Partitions'].remove(partition_id)
            except ValueError:
                pass

            try:
                for part in self.mheart_partitions:
                    if part['TopicID'] == topic_id:
                        part['Partitions'].remove(partition_id)
            except ValueError:
                pass

    @staticmethod
    def _equal(p1, p2):
        """
        compare if p1, p2 partition information are equal
        :return: bool
        """
        if len(p1) != len(p2):
            return False

        for i in range(len(p1)):
            if p1[i]["TopicID"] == p2[i]["TopicID"] and p1[i]["Partitions"] == p2[i]["Partitions"]:
                continue
            else:
                return False

        return True

    @staticmethod
    def _sort(partitions):
        partitions.sort(key=lambda x: x["TopicID"])

    @staticmethod
    def _sub(p1, p2):
        result = []
        same_topics = []
        for pA in p1:
            for pB in p2:
                if pA["TopicID"] == pB["TopicID"]:
                    temp = dict()
                    temp["TopicID"] = pA["TopicID"]
                    temp["Partitions"] = list(set(pA["Partitions"]) - set(pB["Partitions"]))
                    result.extend(temp)
                    same_topics.append(pA["TopicID"])
                    break
        for pA in p1:
            if pA["TopicID"] not in same_topics:
                result.append(pA)

        return result

    @staticmethod
    def _add(p1, p2):
        result = []
        same_topics = []
        for pA in p1:
            for pB in p2:
                if pA["TopicID"] == pB["TopicID"]:
                    temp = dict()
                    temp["TopicID"] = pA["TopicID"]
                    temp["Partitions"] = list(set(pA["Partitions"] + pB["Partitions"]))
                    result.append(temp)
                    same_topics.append(pA["TopicID"])
                    break
        for pA in p1:
            if pA["TopicID"] not in same_topics:
                result.append(pA)
        for pA in p2:
            if pA["TopicID"] not in same_topics:
                result.append(pA)

        return result
