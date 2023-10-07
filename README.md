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
pip install pip install git+https://github.com/TencentCloud/tencentcloud-cls-sdk-python.git

### Host

https://cloud.tencent.com/document/product/614/18940 使用API日志上传域名

### 代码示例

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



