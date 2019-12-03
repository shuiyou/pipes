import os

DEBUG = True

app_env = os.getenv("ENV", 'dev').lower()

# 配置使用的决策引擎
# 获取环境变量，如果没有返回'http://192.168.1.20:8091/rest/S1Public'
STRATEGY_URL = os.getenv('STRATEGY_URL',
                         'http://192.168.1.20:8091/rest/S1Public')

EUREKA_SERVER = os.getenv("EUREKA_SERVER", "http://192.168.1.27:8032/eureka/")

# 通过环境变量配置不同部署环境的是数据库
# GEARS_DB = {
#     'user': os.getenv('DB_USER', 'gears_test'),
#     'pw': os.getenv('DB_PW', 'bd3by68u'),
#     'host': os.getenv('DB_HOST', '192.168.1.9'),
#     'port': os.getenv('DB_PORT', 3360),
#     'db': os.getenv('DB_NAME', 'gears_test'),
# }

# GEARS_DB = {
#     'user': os.getenv('DB_USER', 'gears_test'),
#     'pw': os.getenv('DB_PW', 'bd3by68u'),
#     'host': os.getenv('DB_HOST', '192.168.1.9'),
#     'port': os.getenv('DB_PORT', 3360),
#     'db': os.getenv('DB_NAME', 'gears_test'),
# }

GEARS_DB = {
    'user': os.getenv('DB_USER', 'gears_external'),
    'pw': os.getenv('DB_PW', 'j83eckas'),
    'host': os.getenv('DB_HOST', '192.168.1.9'),
    'port': os.getenv('DB_PORT', 3360),
    'db': os.getenv('DB_NAME', 'gears_external'),
}



