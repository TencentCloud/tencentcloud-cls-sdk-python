日志服务SDK
---
日志服务cls python sdk

### 支持Python版本

    Python 2.7
    Python 3.3
    Python 3.4
    Python 3.5
    Python 3.6
    Python 3.7
    Pypy2
    Pypy3

### 安装

pip install git+https://github.com/TencentCloud/tencentcloud-cls-sdk-python.git

### Host

https://cloud.tencent.com/document/product/614/18940 使用API日志上传域名

### 日志上传代码示例

```
# This is a sample Python script.
import time

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from tencentcloud.log.logclient import LogClient
from tencentcloud.log.logexception import LogException
from tencentcloud.log.cls_pb2 import LogGroupList


def upload(topic_id, client):
    LogLogGroupList = LogGroupList()
    LogGroup = LogLogGroupList.logGroupList.add()
    LogGroup.filename = "python.log"
    LogGroup.source = "127.0.0.1"

    LogTag = LogGroup.logTags.add()
    LogTag.key = "key"
    LogTag.value = "value"

    Log = LogGroup.logs.add()
    Log.time = int(round(time.time() * 1000000))

    Content = Log.contents.add()
    Content.key = "Hello"
    Content.value = "World"
    try:
        request = client.put_log_raw(topic_id, LogLogGroupList)
        print(request.get_request_id())
    except LogException as e:
        print(e)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    endpoint = 'https://ap-guangzhou.cls.tencentcs.com'
    accessKeyId = 'your_access_id'
    accessKey = 'your_access_key'
    topic_id = 'your_project_name'
    client = LogClient(endpoint, accessKeyId, accessKey)
    upload(topic_id, client)

```

### 日志自定义消费代码示例

```
# -*- coding: utf-8 -*-
import json
import os
from threading import RLock
from tencentcloud.log.consumer import *
from tencentcloud.log.logclient import YunApiLogClient

# 消费者处理消费的数据
class SampleConsumer(ConsumerProcessorBase):
    last_check_time = 0
    log_results = []
    lock = RLock()

    def initialize(self, topic_id):
        self.topic_id = topic_id

    # 处理消费的数据
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

                with SampleConsumer.lock:
                    # 数据汇总到SampleConsumer.log_results
                    SampleConsumer.log_results.append(item)

        # 每隔3s提交一次offset
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

    # Worker退出时，会调用该函数，可以在此处执行清理工作
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

# 消费组操作
def sample_consumer_group():
    # 日志服务接入点，请您根据实际情况填写
    endpoint = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_ENDPOINT', '')
    # 访问的地域
    region = ""
    # 用户的Secret_id
    access_key_id = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_ACCESSID', '')
    # 用户的Secret_key
    access_key = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_ACCESSKEY', '')
    # 消费的日志集ID
    logset_id = os.environ.get('TENCENTCLOUD_LOG_SAMPLE_LOGSET_ID', '')
    # 消费的日志主题ID列表，支持多个
    topic_ids = []
    # 消费组名称，同一个日志集下的消费组名称唯一
    consumer_group = 'consumer-group-1'
    # 消费者名称
    consumer_name1 = "consumer-group-1-A"
    consumer_name2 = "consumer-group-1-B"

    assert endpoint and access_key_id and access_key and logset_id, ValueError("endpoint/access_id/access_key and "
                                                                               "logset_id cannot be empty")
    # 创建访问云API接口的Client
    client = YunApiLogClient(access_key_id, access_key, region=region)
    SampleConsumer.log_results = []

    try:
        # 创建两个消费者配置
        option1 = LogHubConfig(endpoint, access_key_id, access_key, region, logset_id, topic_ids, consumer_group,
                               consumer_name1, heartbeat_interval=3, data_fetch_interval=1,
                               offset_start_time=TimePosition.END, max_fetch_log_group_size=1048576)
        option2 = LogHubConfig(endpoint, access_key_id, access_key, region, logset_id, topic_ids, consumer_group,
                               consumer_name2, heartbeat_interval=3, data_fetch_interval=1,
                               offset_start_time=TimePosition.END, max_fetch_log_group_size=1048576)

        # 创建消费者
        print("*** start to consume data...")
        client_worker1 = ConsumerWorker(SampleConsumer, consumer_option=option1)
        client_worker2 = ConsumerWorker(SampleConsumer, consumer_option=option2)

        # 启动消费者
        client_worker1.start()
        client_worker2.start()

        # 等待2分钟，或者获取到数据后继续往后执行
        sleep_until(120, lambda: len(SampleConsumer.log_results) > 0)

        # 关闭消费者
        print("*** stopping workers")
        client_worker1.shutdown()
        client_worker2.shutdown()

        # 打印汇总的日志数据
        print("*** get content:")
        for log in SampleConsumer.log_results:
            print(json.dumps(log))

        # 打印消费组信息：消费组的名称、消费的日志主题、消费者心跳超时时间
        print("*** consumer group status ***")
        ret = client.list_consumer_group(logset_id, topic_ids)
        ret.log_print()

        # 删除消费组
        print("*** delete consumer group")
        time.sleep(30)
        client.delete_consumer_group(logset_id, consumer_group)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e


if __name__ == '__main__':
    sample_consumer_group()
```



