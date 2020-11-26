# 一.正则匹配格式

# 二.返回信息枚举
# resp_mapping = {
#     '01': {
#         "resCode": "1",
#         "resMsg": "失败",
#         "data": {
#             "warningMsg": ['上传文件类型错误,仅支持csv,xls,xlsx格式文件']}
#     },
#     '02': {
#         "resCode": "1",
#         "resMsg": "失败",
#         "data": {
#             "warningMsg": ['上传文件类型错误,仅支持csv,xls,xlsx格式文件']}
#     }
# }


# 三.所有数字配置

# 标题行最大匹配行数
MAX_TITLE_NUMBER = 30
# 最小交易间隔
MIN_TRANS_INTERVAL = 160
# 最小查询间隔
MIN_QUERY_INTERVAL = 180
# 最小导入间隔
MIN_IMPORT_INTERVAL = 45

# 结息日均利率计算参数,不能为0
INTEREST_MULTIPLIER = 0.0035

# 异常交易金额
UNUSUAL_TRANS_AMT = [
    5.20, 5.21, 13.14, 14.13, 20.20, 20.13, 20.14, 131.4, 201.3, 201.4, 520, 520.20, 521, 1314, 1314.2, 1413,
    1413.2, 2013.14, 2014.13, 201314, 2020.2, 202020.2, 13145.2, 1314520, 52013.14, 5201314]
# 家庭不稳定交易密度阈值
UNSTABLE_DENSITY = 0.3

# 最小民间借贷金额
MIN_PRIVATE_LENDING = 500
# 最小民间借贷持续月份数
MIN_CONTI_MONTHS = 6
# 最大民间借贷日期差别
MAX_INTERVAL_DAYS = 5
