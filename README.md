日志服务SDK
---

日志服务cls python sdk

### 支持Python版本

    Python 3.6
    Python 3.7
    Python 3.8
    Python 3.9
    Python 3.10
    Python 3.11
    Python 3.12
    Python 3.13
    Pypy3

### 安装

```bash
pip install git+https://github.com/TencentCloud/tencentcloud-cls-sdk-python.git@v2.0.0
```

### 依赖

| 依赖包 | 版本要求 |
|---|---|
| requests | 最新版 |
| protobuf | >= 4.0.0 |
| six | 最新版 |
| lz4 | >= 4.0.0 |
| python-dateutil | 最新版 |
| python-snappy | <= 0.6.0 |

### Host（接入域名）

日志上传域名格式为：`https://<region>.cls.tencentcs.com`（外网）或 `https://<region>.cls.tencentyun.com`（内网）

完整域名列表请参考：[使用 API 上传日志域名](https://cloud.tencent.com/document/product/614/18940)

#### 支持的地域（Region）

| 常量 | 地域 |
|---|---|
| `Region.BEIJING` | 北京 |
| `Region.GUANGZHOU` | 广州 |
| `Region.SHANGHAI` | 上海 |
| `Region.CHENGDU` | 成都 |
| `Region.NANJING` | 南京 |
| `Region.CHONGQING` | 重庆 |
| `Region.HONGKONG` | 香港 |
| `Region.SINGAPORE` | 新加坡 |
| `Region.TOKYO` | 东京 |
| `Region.SEOUL` | 首尔 |
| `Region.FRANKFURT` | 法兰克福 |
| `Region.SILICONVALLEY` | 硅谷 |
| `Region.ASHBURN` | 弗吉尼亚 |
| `Region.JAKARTA` | 雅加达 |
| `Region.BANGKOK` | 曼谷 |
| `Region.SAOPAULO` | 圣保罗 |

#### 网络类型（NetworkType）

| 常量 | 说明 |
|---|---|
| `NetworkType.EXTRANET` | 外网（`cls.tencentcs.com`） |
| `NetworkType.INTRANET` | 内网（`cls.tencentyun.com`） |

可通过 `EndpointBuilder.createEndpoint(Region, NetworkType)` 快速构建 endpoint：

```python
from tencentcloud.log.endpoint import Region, NetworkType, EndpointBuilder

# 广州地域外网
endpoint = EndpointBuilder.createEndpoint(Region.GUANGZHOU, NetworkType.EXTRANET)
# => ap-guangzhou.cls.tencentcs.com

# 广州地域内网
endpoint = EndpointBuilder.createEndpoint(Region.GUANGZHOU, NetworkType.INTRANET)
# => ap-guangzhou.cls.tencentyun.com
```

### 密钥信息

`accessKeyId` 和 `accessKey` 为云 API 密钥，密钥信息获取请前往 [密钥获取](https://console.cloud.tencent.com/cam/capi)。

请确保密钥关联的账号具有相应的 [SDK 上传日志权限](https://cloud.tencent.com/document/product/614/68374#.E4.BD.BF.E7.94.A8-api-.E4.B8.8A.E4.BC.A0.E6.95.B0.E6.8D.AE)。

### LogClient 初始化

```python
from tencentcloud.log.logclient import LogClient

client = LogClient(
    endpoint,        # 接入域名，例如 https://ap-guangzhou.cls.tencentcs.com
    accessKeyId,     # 云 API SecretId
    accessKey,       # 云 API SecretKey
    securityToken=None,  # 临时密钥 Token（使用临时密钥时填写）
    source=None,     # 日志来源 IP，默认自动获取本机 IP
    region='',       # 地域，例如 ap-guangzhou
)
```

### 日志上传代码示例

#### 方式一：使用 Protobuf 原始结构上传（推荐）

通过 `put_log_raw` 方法直接构造 Protobuf 日志结构上传，性能更优。

```python
# -*- coding: utf-8 -*-
import time

from tencentcloud.log.logclient import LogClient
from tencentcloud.log.logexception import LogException
from tencentcloud.log.cls_pb2 import LogGroup, Log, LogTag


def upload(topic_id, client):
    log_group = LogGroup()
    log_group.filename = "python.log"   # 日志文件名（可选）
    log_group.source = "127.0.0.1"      # 日志来源 IP（可选）

    # 添加日志标签（可选）
    tag = log_group.logTags.add()
    tag.key = "env"
    tag.value = "production"

    # 添加一条日志
    log = log_group.logs.add()
    log.time = int(time.time() * 1000000)  # 微秒时间戳

    content = log.contents.add()
    content.key = "message"
    content.value = "Hello, CLS!"

    content2 = log.contents.add()
    content2.key = "level"
    content2.value = "INFO"

    try:
        response = client.put_log_raw(topic_id, log_group)
        print("上传成功，RequestId:", response.get_request_id())
    except LogException as e:
        print("上传失败，错误码:", e.get_error_code(), "，错误信息:", e.get_error_message())


if __name__ == '__main__':
    endpoint = 'https://ap-guangzhou.cls.tencentcs.com'
    # 也可以通过 EndpointBuilder 构建 endpoint
    # from tencentcloud.log.endpoint import Region, NetworkType, EndpointBuilder
    # endpoint = EndpointBuilder.createEndpoint(Region.GUANGZHOU, NetworkType.EXTRANET)

    accessKeyId = 'your_secret_id'
    accessKey = 'your_secret_key'
    topic_id = 'your_topic_id'

    client = LogClient(endpoint, accessKeyId, accessKey)
    upload(topic_id, client)
```

#### 方式二：批量上传多条日志

```python
# -*- coding: utf-8 -*-
import time

from tencentcloud.log.logclient import LogClient
from tencentcloud.log.logexception import LogException
from tencentcloud.log.cls_pb2 import LogGroup


def batch_upload(topic_id, client):
    log_group = LogGroup()
    log_group.source = "127.0.0.1"

    # 批量添加多条日志
    log_data = [
        {"message": "用户登录", "user": "alice", "level": "INFO"},
        {"message": "查询失败", "user": "bob",   "level": "ERROR"},
        {"message": "订单创建", "user": "carol", "level": "INFO"},
    ]

    for item in log_data:
        log = log_group.logs.add()
        log.time = int(time.time() * 1000000)
        for k, v in item.items():
            content = log.contents.add()
            content.key = k
            content.value = v

    try:
        response = client.put_log_raw(topic_id, log_group)
        print("批量上传成功，RequestId:", response.get_request_id())
    except LogException as e:
        print("上传失败，错误码:", e.get_error_code(), "，错误信息:", e.get_error_message())


if __name__ == '__main__':
    endpoint = 'https://ap-guangzhou.cls.tencentcs.com'
    accessKeyId = 'your_secret_id'
    accessKey = 'your_secret_key'
    topic_id = 'your_topic_id'

    client = LogClient(endpoint, accessKeyId, accessKey)
    batch_upload(topic_id, client)
```

### put_log_raw 接口说明

| 参数 | 类型 | 说明 |
|---|---|---|
| `topic_id` | string | 日志主题 ID |
| `log_group` | LogGroup | Protobuf 日志组对象 |

**返回值**：`PutLogsResponse`，可通过 `response.get_request_id()` 获取请求 ID。

**LogGroup 字段说明**：

| 字段 | 类型 | 说明 |
|---|---|---|
| `source` | string | 日志来源，通常为机器 IP |
| `filename` | string | 日志文件名（可选） |
| `logTags` | repeated LogTag | 日志标签列表（可选） |
| `logs` | repeated Log | 日志列表 |

**Log 字段说明**：

| 字段 | 类型 | 说明 |
|---|---|---|
| `time` | int64 | 日志时间，微秒时间戳 |
| `contents` | repeated Content | 日志内容键值对列表 |

### 异常处理

```python
from tencentcloud.log.logexception import LogException

try:
    response = client.put_log_raw(topic_id, log_group)
except LogException as e:
    print("错误码:", e.get_error_code())
    print("错误信息:", e.get_error_message())
    print("RequestId:", e.get_request_id())
    print("HTTP 状态码:", e.resp_status)
```

常见错误码：

| 错误码 | 说明 |
|---|---|
| `AuthFailure` | 鉴权失败，请检查 SecretId/SecretKey 是否正确 |
| `InvalidParameter` | 参数错误，请检查 topic_id 等参数 |
| `ResourceNotFound` | 日志主题不存在 |
| `SpeedQuotaExceed` | 写入超过配额限制，SDK 会自动重试 |
| `InternalError` | 服务端内部错误，SDK 会自动重试 |

### 日志自定义消费代码示例

> 推荐使用 3.6 及以上 python 版本进行数据消费

```python
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
