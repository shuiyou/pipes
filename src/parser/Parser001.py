# @Time : 2020/6/18 6:45 PM 
# @Author : lixiaobo
# @File : Parser001.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
from parser.Parser import Parser

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
        logger.info("流水报告解析及验真参数:param:%s", self.param)
        logger.info("流水报告解析及验真参数:file:%s", self.file)
        return "----"
