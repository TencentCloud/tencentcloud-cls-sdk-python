# -*- coding: utf-8 -*-

import abc
import logging
import sys
import time

import six

from tencentcloud.log.logexception import LogException

logger = logging.getLogger(__name__)


class ConsumerProcessorBase(object):
    def __init__(self):
        self.topic_id = ''
        self.partition_id = -1
        self.last_check_time = 0
        self.offset_timeout = 3

    def save_offset(self, tracker, force=False):
        current_time = time.time()
        if force or current_time - self.last_check_time > self.offset_timeout:
            try:
                self.last_check_time = current_time
                tracker.save_offset(True)
            except Exception as ex:
                logger.warning(
                    "Fail to store offset for partition {0}:{1}, error: {2}".format(self.topic_id, self.partition_id,
                                                                                    ex))
        else:
            try:
                tracker.save_offset(False)
            except Exception as ex:
                logger.warning(
                    "Fail to store offset for partition {0}:{1}, error: {2}".format(self.topic_id, self.partition_id,
                                                                                    ex))

    def initialize(self, topic_id):
        self.topic_id = topic_id

    @abc.abstractmethod
    def process(self, log_groups, offset_tracker):
        raise NotImplementedError('not create method process')

    def shutdown(self, offset_tracker):
        consumer_client = offset_tracker.consumer_group_client
        _id = '/'.join([
            consumer_client.logset_id, self.topic_id,
            consumer_client.consumer_group, consumer_client.consumer,
            str(self.partition_id)
        ])
        logger.info("[%s]ConsumerProcesser is shutdown, partition id: %s", _id,
                    self.partition_id)
        self.save_offset(offset_tracker, force=True)


class ConsumerProcessorAdaptor(ConsumerProcessorBase):
    def __init__(self, func):
        super(ConsumerProcessorAdaptor, self).__init__()
        self.func = func

    def process(self, log_groups, offset_tracker):
        ret = self.func(self.topic_id, self.partition_id, log_groups)
        if isinstance(ret, bool) and not ret:
            return  # do not save offset when getting False

        self.save_offset(offset_tracker)


class TaskResult(object):
    def __init__(self, task_exception):
        self.task_exception = task_exception
        self._exc_info = None
        if six.PY2 and task_exception is not None:
            self._exc_info = sys.exc_info()

    def get_exception(self):
        return self.task_exception

    @property
    def exc_info(self):
        if six.PY3:
            return self.task_exception
        else:
            return self._exc_info


class ProcessTaskResult(TaskResult):
    def __init__(self, rollback_offset):
        super(ProcessTaskResult, self).__init__(None)
        self.rollback_offset = rollback_offset

    def get_rollback_offset(self):
        return self.rollback_offset


class InitTaskResult(TaskResult):
    def __init__(self, offset, offset_persistent):
        super(InitTaskResult, self).__init__(None)
        self.offset = offset
        self.offset_persistent = offset_persistent

    def get_offset(self):
        return self.offset

    def is_offset_persistent(self):
        return self.offset_persistent


class FetchTaskResult(TaskResult):
    def __init__(self, fetched_log_groups, offset):
        super(FetchTaskResult, self).__init__(None)
        self.fetched_log_groups = fetched_log_groups
        self.offset = offset

    def get_fetched_log_group_list(self):
        return self.fetched_log_groups

    def get_offset(self):
        return self.offset


def consumer_process_task(processor, log_groups, offset_tracker):
    """
    return TaskResult if failed,
    return ProcessTaskResult if succeed
    :param processor:
    :param log_groups:
    :param offset_tracker:
    :return:
    """
    try:
        offset = processor.process(log_groups, offset_tracker)
        offset_tracker.flush_check()
    except Exception as e:
        return TaskResult(e)
    return ProcessTaskResult(offset)


def consumer_initialize_task(processor, consumer_client, topic_id, partition_id, offset_start_time,
                             offset_end_time=None):
    """
    return TaskResult if failed, or else, return InitTaskResult
    :param processor:
    :param consumer_client:
    :param topic_id:
    :param partition_id:
    :param offset_start_time:
    :param offset_end_time:
    :return:
    """
    try:
        processor.initialize(topic_id)
        is_offset_persistent = False
        c_offset = consumer_client.get_partition_offsets(topic_id, partition_id, offset_start_time)
        offset = -1
        if c_offset > 0:
            is_offset_persistent = True
            offset = c_offset

        return InitTaskResult(offset, is_offset_persistent)
    except Exception as e:
        return TaskResult(e)


def consumer_fetch_task(loghub_client_adapter, topic_id, partition_id, offset, max_fetch_log_group_size=1000,
                        end_time=None):
    exception = None

    for retry_times in range(3):
        try:
            response = loghub_client_adapter.pull_logs(loghub_client_adapter.consumer_group, topic_id, partition_id,
                                                       max_fetch_log_group_size, offset=offset, end_time=end_time)
            fetch_log_groups = response.get_log_groups()
            next_offset = response.get_next_offset()
            logger.debug("topic id = %s partition id = %s offset = %s next offset = %s size: %d",
                         topic_id, partition_id, offset, next_offset,
                         response.get_log_count())
            if not next_offset:
                return FetchTaskResult(fetch_log_groups, offset)
            else:
                return FetchTaskResult(fetch_log_groups, next_offset)
        except LogException as e:
            exception = e
        except Exception as e1:
            logger.error(e1, exc_info=True)
            raise Exception(e1)

        if retry_times == 0 and isinstance(exception, LogException) \
                and 'invalidoffset' in exception.get_error_code().lower():
            try:
                offset = loghub_client_adapter.get_offsets(topic_id, partition_id, "end")
            except Exception:
                return TaskResult(exception)
        else:
            break

    return TaskResult(exception)


def consumer_shutdown_task(processor, offset_tracker):
    """
    :param processor:
    :param offset_tracker:
    :return:
    """
    exception = None
    try:
        processor.shutdown(offset_tracker)
    except Exception as e:
        print(e)
        exception = None

    try:
        offset_tracker.get_offset()
    except Exception:
        logger.error('Failed to flush check point', exc_info=True)

    return TaskResult(exception)
