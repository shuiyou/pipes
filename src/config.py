import os

DEBUG = True

app_env = os.getenv("ENV", 'test').lower()

# 配置使用的决策引擎
STRATEGY_URL = os.getenv('STRATEGY_URL',
                         'http://192.168.1.20:8091/rest/S1Public')  ## 获取环境变量，如果没有返回'http://192.168.1.20:8091/rest/S1Public'

# 通过环境变量配置不同部署环境的是数据库
GEARS_DB = {
    'user': os.getenv('GEARS_USER', 'gears_dev'),
    'pw': os.getenv('GEARS_PW', 'x2cqpau4'),
    'host': os.getenv('GEARS_HOST', '192.168.1.9'),
    'port': os.getenv('GEARS_PORT', 3360),
    'db': os.getenv('DB_NAME', 'gears_dev'),
}
