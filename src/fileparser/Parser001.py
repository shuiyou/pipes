# @Time : 2020/6/18 6:45 PM 
# @Author : lixiaobo
# @File : Parser001.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
from fileparser.Parser import Parser

# 流水报告解析及验真
logger = LoggerUtil().logger(__name__)


class Parser001(Parser):
    def __init__(self):
        super().__init__()

    # 解析，验真逻辑， 此成员变量
    # self.param  提交的入参
    # self.file   待解析的文件
    # 返回解析验真结果
    def process(self):
        """
            入参： param：
                {
                    'appId': '商户端ID',
                    'cusType': "主体类型：PERSONAL, COMPANY",
                    'cusName': "客户名称",
                    'idNo': "证件号",
                    'idType': "证件号码ID_CARD_NO, CREDIT_CODE, REG_NO,",
                    'bankAccount': "银行账号",
                    'bankName': "银行名",
                    'outApplyNo': "申请业务号",
                    'outReqNo': "外部请求编号",
                    'bizReqNo': "回执编号",
                    'accountId': "账户编号"
                }
            返回值：resCode：
                  0 成功
                  1 失败
                  2 异常
                  3 处理中
                  10 参数错误
                  20 解析失败
                  21 校验失败
                  22 验真失败
        """
        logger.info("流水报告解析及验真参数:param:%s", self.param)
        logger.info("流水报告解析及验真参数:file:%s", self.file)

        resp = {
            "resCode": "0",
            "resMsg": "操作成功",
            "data": {
                "key1": "value1",
                "key2": "value2"
            }
        }

        return resp
