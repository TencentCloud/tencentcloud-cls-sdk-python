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
    Python 3.8
    Python 3.9
    Python 3.10
    Python 3.11
    Python 3.12
    Python 3.13
    Pypy2
    Pypy3

### 安装

`pip install git+https://github.com/TencentCloud/tencentcloud-cls-sdk-python.git@v1.0.7`

### Host

`https://cloud.tencent.com/document/product/614/18940` 使用API日志上传域名

### 密钥信息

accessKeyId和accessKey为云API密钥，密钥信息获取请前往[密钥获取](https://console.cloud.tencent.com/cam/capi)
。并请确保密钥关联的账号具有相应的[SDK上传日志权限](https://cloud.tencent.com/document/product/614/68374#.E4.BD.BF.E7.94.A8-api-.E4.B8.8A.E4.BC.A0.E6.95.B0.E6.8D.AE)

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
    #endpoint = EndpointBuilder.createEndpoint(Region.GUANGZHOU, NetworkType.INTRANET) #也可以通过选择地域和网络类型组成endpoint，且支持自定义参数传入
    accessKeyId = 'your_access_id'
    accessKey = 'your_access_key'
    topic_id = 'your_project_name'
    client = LogClient(endpoint, accessKeyId, accessKey)
    upload(topic_id, client)

```

### 日志自定义消费代码示例

> 推荐使用 3.5 及以上 python 版本进行数据消费

```
# -*- coding: utf-8 -*-
import json
import os
import signal

from tencentcloud.log.consumer import *

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
        # 预过滤条件 (通过配置 query 参数来实现预过滤再消费, 不配置这个参数代表全量消费日志)
        # query的例子: log_keep(op_and(op_gt(v("status"), 400), str_exist(v("cdb_message"), "pwd")))
        # 实现的效果:仅消费 status 大于400且 cdb_message 字段包含 "pwd" 的日志
        # 过滤语法参考文档：https://cloud.tencent.com/document/product/614/39262
        self.query = '您的过滤条件'
        # consumer group name
        self.consumer_group = 'consumer-group-1'
        # consumer id
        self.consumer_name1 = "consumer-group-1-A"
        assert self.endpoint and self.access_key_id and self.access_key and self.logset_id, (
            "endpoint/access_id/access_key and logset_id cannot be empty"
        )
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
            print(f"*** 使用查询过滤条件: {self.query}")

            # consumer config
            option1 = LogHubConfig(self.endpoint, self.access_key_id, self.access_key, self.region, self.logset_id,
                                   self.topic_ids, self.consumer_group,
                                   self.consumer_name1, heartbeat_interval=3, data_fetch_interval=1,
                                   offset_start_time='begin', max_fetch_log_group_size=1048576, query=self.query)
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
```
