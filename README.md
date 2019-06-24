# 决策引擎数据映射服务
使用python来解决数据转换到决策引擎需要的数据结构。 本项目基于flask，pandas等框架实现。
[flask大概介绍](https://www.cnblogs.com/franknihao/p/7118469.html)
[pandas exercises](https://github.com/guipsamora/pandas_exercises)


## 决策前置的输入及输出
1. 输入请求：
```json
{
  "productCode": "产品编码",
  "reqNo": "xxxx",
  "queryData": {
    "name": "",
    "idno": "",
    "phone": ""
  }
   
}

```

## Usage

1. 本地安装好git
2. 使用ssh-keygen生成好id_rsa.pub, [把里面的内容放到gitlab上的ssh key配置里](https://www.jianshu.com/p/4f5cb637eff7)
3. 运行`git clone git@192.168.1.8:transformer/pipes.git` 把项目clone到你本地
4. [本地环境安装好python3.x的环境](https://www.anaconda.com/distribution/)请选择3.x版本
5. 到你的项目目录下运行 `pip install -r requirements.txt` 安装项目依赖的包
6. python代码写到app目录下
```
.
├── Dockerfile
├── __pycache__
│   ├── app.cpython-36.pyc
│   ├── app.cpython-37.pyc
│   ├── config.cpython-36.pyc
│   └── config.cpython-37.pyc
├── app.py
├── config.py
├── echarts
│   ├── __init__.py
│   ├── bar.py
│   └── scatter.py
├── exceptions.py
├── logger
│   ├── __init__.py
│   ├── __pycache__
│   ├── logger_util.py
│   ├── logging-dev.conf
│   ├── logging-fat.conf
│   ├── logging-prod.conf
│   └── logstash.py
├── mapping
│   ├── __init__.py
│   └── mysql_reader.py
├── requirements.txt
└── strategy
    ├── __init__.py
    ├── request.py
    └── response.py

6 directories, 23 files
 luokui@MacBook-Pro  ~/work/pipes/src   develop ●  cd ..                                                        ✔  3922  14:33:11
 luokui@MacBook-Pro  ~/work/pipes   develop ●  tree . -L 2                                                      ✔  3923  14:33:32
.
├── README.md
├── build.sh
├── docker-compose.yaml
├── docs
│   └── index.md
├── src
│   ├── Dockerfile
│   ├── __pycache__
│   ├── app.py
│   ├── config.py
│   ├── echarts
│   ├── exceptions.py
│   ├── logger
│   ├── mapping
│   ├── requirements.txt
│   └── strategy
└── tests
    ├── __init__.py
    ├── data
    ├── logstash.db
    ├── metabase
    ├── strategyone.json
    ├── td_json.py
    ├── test_app.py
    └── test_strategy.py

```
   
```bash
$ docker-compose up -d --no-recreate --scale app=5
```

### 开发流程
使用gitflow分支管理策略

推荐使用pycharm进行开发


### 单元测试
1. [如何使用pycharm运行单元测试](https://blog.csdn.net/chenmozhe22/article/details/81700504)
1. 使用vscode 需要在.vscode/settings.json文件里添加如下内容,需要设置你本地的python3.7的路径
```
   {
    "python.pythonPath": "/usr/local/opt/python/bin/python3.7",
    "python.testing.pyTestArgs": [
        "test"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.nosetestsEnabled": false,
    "python.testing.pyTestEnabled": true
}
```

* 使用faker和[faker2db](https://github.com/emirozer/fake2db)来构建测试数据

2. 安装python test插件

`pip3 install pytest`
### 日志管理
日志会打到logstash里去， 使用 [python-logstash-async](https://python-logstash-async.readthedocs.io/en/stable/config.html)

日志会根据系统的环境变量'ENV'设置的值来选择对应环境的配置。
ENV可设置为 DEV，FAT，UAT，PROD