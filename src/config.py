import os

DEBUG = True

app_env = os.getenv("ENV", 'dev').lower()

# 配置使用的决策引擎
STRATEGY_URL = os.getenv('STRATEGY_URL',
                         'http://192.168.1.20:8091/rest/S1Public')  ## 获取环境变量，如果没有返回'http://192.168.1.20:8091/rest/S1Public'

# 通过环境变量配置不同部署环境的是数据库
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

# 湛泸产品编码和决策process的对应关系
product_code_process_dict = {
    "001": "Level1_m",  # 一级个人报告
    "002": "Level1_m",  # 一级企业
    "003": "Level1_m"
}

# 在这个dict里针对不同的产品设置他的详情字段转换
product_code_view_dict = {
    # 一级个人报告
    "001": ['10001', '13001', '12001', '17001', '07001', '09001', 'f0003'],
}
