# -*- coding: utf-8 -*-

import copy
import logging
import time

from tencentcloud.log.consumer.config import ConsumerStatus
from tencentcloud.log.consumer.exceptions import ClientWorkerException
from tencentcloud.log.consumer.fetched_log_group import FetchedLogGroup
from tencentcloud.log.consumer.offset_tracker import ConsumerOffsetTracker
from tencentcloud.log.consumer.tasks import ProcessTaskResult, InitTaskResult, FetchTaskResult, TaskResult
from tencentcloud.log.consumer.tasks import consumer_fetch_task, consumer_initialize_task, \
    consumer_process_task, consumer_shutdown_task


class PartitionConsumerWorkerLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        partition_consumer_worker = self.extra['partition_consumer_worker']  # type: PartitionConsumerWorker
        consumer_client = partition_consumer_worker.log_client
        _id = '/'.join([
            consumer_client.logset_id, partition_consumer_worker.topic_id,
            consumer_client.consumer_group, consumer_client.consumer,
            str(partition_consumer_worker.partition_id)
        ])
        return "[{0}] {1}".format(_id, msg), kwargs


class PartitionConsumerWorker(object):
    def __init__(self, log_client, topic_id, partition_id, consumer_name, processor, offset_start_time,
                 max_fetch_log_group_size=1000, executor=None, offset_end_time=None, query=None):
        self.topic_id = topic_id
        self.log_client = log_client
        self.partition_id = partition_id
        self.consumer_name = consumer_name
        self.offset_start_time = offset_start_time
        self.offset_end_time = offset_end_time or None
        self.processor = processor
        self.offset_tracker = ConsumerOffsetTracker(self.log_client, self.consumer_name, self.topic_id,
                                                    self.partition_id)
        self.executor = executor
        self.max_fetch_log_group_size = max_fetch_log_group_size

        self.consumer_status = ConsumerStatus.INITIALIZING
        self.current_task_exist = False
        self.task_future = None
        self.fetch_data_future = None

        self.next_fetch_offset = ''

        self.shutdown = False
        self.last_fetch_log_group = None

        self.last_log_error_time = 0
        self.last_fetch_time = 0
        self.last_fetch_count = 0
        self.last_success_fetch_time = 0
        self.last_success_fetch_time_with_data = 0
        self.save_last_offset = False
        self.fetch_reach_end = False
        self.next_task_reach_end = False
        self.query = query

        self.logger = PartitionConsumerWorkerLoggerAdapter(
            logging.getLogger(__name__), {"partition_consumer_worker": self})

    def consume(self):
        self.logger.debug('consumer start consuming')
        self.check_and_generate_next_task()
        if self.consumer_status == ConsumerStatus.PROCESSING and self.last_fetch_log_group is None:
            self.fetch_data()

    @staticmethod
    # get future (if failed return None)
    def get_task_result(task_future):
        if task_future is not None and task_future.done():
            try:
                return task_future.result()
            except Exception:
                import traceback
                traceback.print_exc()
        return None

    def fetch_data(self):
        # no task or it's done
        if self.fetch_data_future is None or self.fetch_data_future.done():
            task_result = self.get_task_result(self.fetch_data_future)

            # task is done, output results and get next_offset
            if task_result is not None and task_result.get_exception() is None:
                assert isinstance(task_result, FetchTaskResult), \
                    ClientWorkerException("fetch result type is not as expected")

                self.last_success_fetch_time = time.time()

                self.last_fetch_log_group = FetchedLogGroup(self.partition_id, task_result.get_fetched_log_group_list(),
                                                            task_result.get_offset())
                self.next_fetch_offset = task_result.get_offset()
                self.fetch_reach_end = task_result.get_reach_end()
                self.last_fetch_count = self.last_fetch_log_group.log_group_size
                if self.last_fetch_count > 0:
                    self.last_success_fetch_time_with_data = time.time()
                    self.save_last_offset = False
                else:
                    if self.last_success_fetch_time_with_data != 0 and time.time() - self.last_success_fetch_time_with_data > 30 \
                            and not self.save_last_offset:
                        try:
                            self.offset_tracker.flush_offset()
                        except Exception:
                            import traceback
                            traceback.print_exc()
                        self.save_last_offset = True

            self._sample_log_error(task_result)

            # no task or task is done, create new task
            if task_result is None or task_result.get_exception() is None:
                # flag to indicate if it's done
                is_generate_fetch_task = True

                # throttling control
                if self.last_fetch_count < 100:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.5
                elif self.last_fetch_count < 500:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.2
                elif self.last_fetch_count < 1000:
                    is_generate_fetch_task = (time.time() - self.last_fetch_time) > 0.05

                if is_generate_fetch_task and not self.fetch_reach_end:
                    self.last_fetch_time = time.time()
                    self.fetch_data_future = \
                        self.executor.submit(consumer_fetch_task,
                                             self.log_client, self.topic_id, self.partition_id, self.next_fetch_offset,
                                             max_fetch_log_group_size=self.max_fetch_log_group_size,
                                             end_time=self.offset_end_time, query=self.query)
                else:
                    self.fetch_data_future = None
            else:
                self.fetch_data_future = None

    def check_and_generate_next_task(self):
        """
        check if the previous task is done and proceed to fire another task
        :return:
        """

        if self.task_future is None or self.task_future.done():
            task_success = False

            task_result = self.get_task_result(self.task_future)
            self.task_future = None

            if task_result is not None and task_result.get_exception() is None:

                task_success = True
                if isinstance(task_result, InitTaskResult):
                    # maintain check points
                    assert self.consumer_status == ConsumerStatus.INITIALIZING, \
                        ClientWorkerException("get init task result, but status is: " + str(self.consumer_status))

                    init_result = task_result
                    self.next_fetch_offset = init_result.get_offset()
                    self.offset_tracker.set_memory_offset(self.next_fetch_offset)
                    if init_result.is_offset_persistent():
                        self.offset_tracker.set_persistent_offset(self.next_fetch_offset)

                elif isinstance(task_result, ProcessTaskResult):
                    # maintain check points
                    process_task_result = task_result
                    # next task reach endTime, shutdown worker
                    if self.fetch_reach_end:
                        self.next_task_reach_end = self.fetch_reach_end
                        try:
                            self.offset_tracker.flush_offset(force=True)
                        except Exception:
                            import traceback
                            traceback.print_exc()

                    roll_back_offset = process_task_result.get_rollback_offset()
                    if roll_back_offset:
                        self.last_fetch_log_group = None
                        self.logger.info("user defined to roll-back check-point, cancel current fetching task")
                        self.cancel_current_fetch()
                        self.next_fetch_offset = roll_back_offset

            # log task status
            self._sample_log_error(task_result)

            # update status basing on task results
            self._update_status(task_success)

            self._generate_next_task()

    def _generate_next_task(self):
        """
        submit consumer framework defined task
        :return:
        """
        if self.consumer_status == ConsumerStatus.INITIALIZING:
            self.current_task_exist = True
            self.task_future = self.executor.submit(consumer_initialize_task, self.processor, self.log_client,
                                                    self.topic_id, self.partition_id,
                                                    self.offset_start_time, self.offset_end_time)

        elif self.consumer_status == ConsumerStatus.PROCESSING:
            if self.last_fetch_log_group is not None:
                self.offset_tracker.set_offset(self.last_fetch_log_group.end_offset)
                self.current_task_exist = True

                # must deep copy cause some revision will happen
                last_fetch_log_group = copy.deepcopy(self.last_fetch_log_group)
                self.last_fetch_log_group = None

                if self.last_fetch_count > 0:
                    self.task_future = self.executor.submit(consumer_process_task, self.processor,
                                                            last_fetch_log_group.fetched_log_groups,
                                                            self.offset_tracker)

        elif self.consumer_status == ConsumerStatus.SHUTTING_DOWN:
            self.current_task_exist = True
            self.logger.info("start to cancel fetch job")
            self.cancel_current_fetch()
            self.task_future = self.executor.submit(consumer_shutdown_task, self.processor, self.offset_tracker)

    def cancel_current_fetch(self):
        if self.fetch_data_future is not None:
            self.fetch_data_future.cancel()
            self.logger.info('Cancel a fetch task, partition id: {0}:{1}'.format(self.topic_id, self.partition_id))
            self.fetch_data_future = None

    def _sample_log_error(self, result):
        # record the time when error happens
        if not isinstance(result, TaskResult):
            return

        exc = result.get_exception()
        if exc is None:
            return

        current_time = time.time()
        if current_time - self.last_log_error_time <= 5:
            return

        self.logger.warning(exc, exc_info=result.exc_info)
        self.last_log_error_time = current_time

    def _update_status(self, task_succcess):
        if self.consumer_status == ConsumerStatus.SHUTTING_DOWN:
            # if no task or previous task suceeds, shutting-down -> shutdown complete
            if not self.current_task_exist or task_succcess:
                self.consumer_status = ConsumerStatus.SHUTDOWN_COMPLETE
        elif self.shutdown:
            # always set to shutting down (if flag is set)
            self.consumer_status = ConsumerStatus.SHUTTING_DOWN
        elif self.consumer_status == ConsumerStatus.INITIALIZING:
            # if initing and has task succeed, init -> processing
            if task_succcess:
                self.consumer_status = ConsumerStatus.PROCESSING

    def shut_down(self):
        """
        set shutting down flag, if not shutdown yet, complete the ongoing task(?)
        :return:
        """
        self.shutdown = True
        if not self.is_shutdown():
            self.check_and_generate_next_task()

    def is_shutdown(self):
        return self.consumer_status == ConsumerStatus.SHUTDOWN_COMPLETE
