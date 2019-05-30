# 决策引擎数据映射服务
使用python来解决数据转换到决策引擎需要的数据结构
## Usage
```bash
$ docker-compose up -d --no-recreate --scale app=5
```


### 日志管理
日志会打到logstash里去， 使用 [python-logstash-async](https://python-logstash-async.readthedocs.io/en/stable/config.html)