日志服务SDK
---

### 支持Python版本


### 安装


### 代码示例

```
topic_id=""
access_key_id=""
access_key=""
endpoint=""

client = new LogClient(endpoint, access_key_id, access_key)
# 构建protoBuf日志内容
LogLogGroupList = cls.LogGroupList()

LogGroup = LogLogGroupList.logGroupList.add()
LogGroup.contextFlow = "1"
LogGroup.filename = "python.log"
LogGroup.source = "localhost"

LogTag = LogGroup.logTags.add()
LogTag.key = "key"
LogTag.value = "value"

Log = LogGroup.logs.add()
Log.time = int(round(time.time() * 1000000))

Content = Log.contents.add()
Content.key = "Hello"
Content.value = "World"

client.put_log_raw(topic_id, LogLogGroupList)
```



