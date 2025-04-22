# -*- coding: utf-8 -*-
import json
import os
import signal
from threading import RLock

from tencentcloud.log.consumer import *
from tencentcloud.log.logclient import YunApiLogClient

# logger
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

    def initialize(self, topic_id):
        self.topic_id = topic_id

    def process(self, log_groups, offset_tracker):
        for log_group in log_groups:
            for log in log_group.logs:
                # 处理单行数据
                item = dict()
                item['filename'] = log_group.filename
                item['source'] = log_group.source
                item['time'] = log.time
                for content in log.contents:
                    item[content.key] = content.value

                # Subsequent data processing
                # put your business logic here
                print(json.dumps(item))

        # offset commit
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


class App:
    def __init__(self):
        self.shutdown_flag = False
        # access endpoint
        self.endpoint = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_ENDPOINT', '')
        # region
        self.region = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_REGION', '')
        # secret id
        self.access_key_id = os.environ.get(
            'TENCENTCLOUD_LOG_SAMPLE_ACCESSID', '')
        # secret key
        self.access_key = os.environ.get(
            'TENCENTCLOUD_LOG_SAMPLE_ACCESSKEY', '')
        # logset id
        self.logset_id = os.environ.get(
            'TENCENTCLOUD_LOG_SAMPLE_LOGSET_ID', '')
        # topic ids
        self.topic_ids = os.environ.get(
            'TENCENTCLOUD_LOG_SAMPLE_TOPICS', '').split(',')
        # consumer group name
        self.consumer_group = 'consumer-group-1'
        # consumer id
        self.consumer_name1 = "consumer-group-1-A"
        assert self.endpoint and self.access_key_id and self.access_key and self.logset_id, ValueError("endpoint/access_id/access_key and "
                                                                                                       "logset_id cannot be empty")
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        print(f"catch signal {signum}，cleanup...")
        self.shutdown_flag = True

    def run(self):
        print("*** start to run consumer...")
        self.consume()
        # waiting for exit signal
        while not self.shutdown_flag:
            time.sleep(1)
        # shutdown consumer
        print("*** stopping workers")
        self.consumer.shutdown()
        sys.exit(0)

    def consume(self):
        try:
            # consumer config
            option1 = LogHubConfig(self.endpoint, self.access_key_id, self.access_key, self.region, self.logset_id, self.topic_ids, self.consumer_group,
                                   self.consumer_name1, heartbeat_interval=3, data_fetch_interval=1,
                                   offset_start_time='begin', max_fetch_log_group_size=1048576)
            # init consumer
            self.consumer = ConsumerWorker(
                SampleConsumer, consumer_option=option1)

            # start consumer
            print("*** start to consume data...")
            self.consumer.start()
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise e


if __name__ == '__main__':
    app = App()
    app.run()
