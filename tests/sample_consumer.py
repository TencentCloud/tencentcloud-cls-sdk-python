# -*- coding: utf-8 -*-

import os
from threading import RLock

from tencentcloud.log.consumer import *
from tencentcloud.log.logclient import YunApiLogClient

root = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    fmt='[%(asctime)s] - [%(threadName)s] - {%(module)s:%(funcName)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'))
root.setLevel(logging.INFO)
root.addHandler(handler)

logger = logging.getLogger(__name__)


class SampleConsumer(ConsumerProcessorBase):
    last_check_time = 0
    log_results = []
    lock = RLock()

    def initialize(self, topic_id):
        self.topic_id = topic_id

    def process(self, log_groups, offset_tracker):
        for log_group in log_groups:
            for log in log_group.logs:
                item = dict()
                item['time'] = log.time
                item['filename'] = log_group.filename
                item['source'] = log_group.source
                for content in log.contents:
                    item[content.key] = content.value

                with SampleConsumer.lock:
                    SampleConsumer.log_results.append(item)

        current_time = time.time()
        if current_time - self.last_check_time > 3:
            try:
                self.last_check_time = current_time
                offset_tracker.save_offset(True)
            except Exception:
                import traceback
                traceback.print_exc()
        else:
            try:
                offset_tracker.save_offset(False)
            except Exception:
                import traceback
                traceback.print_exc()

        return None

    def shutdown(self, offset_tracker):
        try:
            offset_tracker.save_offset(True)
        except Exception:
            import traceback
            traceback.print_exc()


def sleep_until(seconds, exit_condition=None, expect_error=False):
    if not exit_condition:
        time.sleep(seconds)
        return

    s = time.time()
    while time.time() - s < seconds:
        try:
            if exit_condition():
                break
        except Exception:
            if expect_error:
                continue
        time.sleep(1)


def sample_consumer_group():
    # load options from envs
    endpoint = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_ENDPOINT', '')
    access_key_id = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_ACCESSID', '')
    access_key = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_ACCESSKEY', '')
    logset_id = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_LOGSET_ID', '')
    topic_ids = []
    consumer_group = 'consumer-group-1'
    consumer_name1 = "consumer-group-1-A"
    consumer_name2 = "consumer-group-1-B"
    token = ""
    region = ""

    assert endpoint and access_key_id and access_key and logset_id, ValueError("endpoint/access_id/access_key and "
                                                                               "logset_id cannot be empty")
    client = YunApiLogClient(access_key_id, access_key, region=region)
    SampleConsumer.log_results = []

    try:
        # create two consumers in the consumer group
        option1 = LogHubConfig(endpoint, access_key_id, access_key, region, logset_id, topic_ids, consumer_group,
                               consumer_name1, heartbeat_interval=3, data_fetch_interval=1,
                               offset_start_time="end", max_fetch_log_group_size=10485760)
        option2 = LogHubConfig(endpoint, access_key_id, access_key, region, logset_id, topic_ids, consumer_group,
                               consumer_name2, heartbeat_interval=3, data_fetch_interval=1,
                               offset_start_time="end", max_fetch_log_group_size=10485760)

        print("*** start to consume data...")
        client_worker1 = ConsumerWorker(SampleConsumer, consumer_option=option1)
        client_worker1.start()
        client_worker2 = ConsumerWorker(SampleConsumer, consumer_option=option2)
        client_worker2.start()

        sleep_until(120, lambda: len(SampleConsumer.log_results) > 0)

        print("*** consumer group status ***")
        ret = client.list_consumer_group(logset_id, topic_ids)
        ret.log_print()

        print("*** stopping workers")
        client_worker1.shutdown()
        client_worker2.shutdown()

        print("*** delete consumer group")
        client.delete_consumer_group(logset_id, consumer_group)
    except Exception as e:
        raise e

    # validate
    ret = str(SampleConsumer.log_results)
    print("*** get content:")
    print(ret)


if __name__ == '__main__':
    sample_consumer_group()
