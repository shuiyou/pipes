# @Time : 2020/4/21 11:31 AM 
# @Author : lixiaobo
# @File : p07002.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
from product.generate import Generate
from product.p07001 import P07001

logger = LoggerUtil().logger(__name__)


# 征信拦截的处理
class P07002(P07001):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}
