# @Time : 2020/6/18 8:00 PM 
# @Author : lixiaobo
# @File : p08001.py 
# @Software: PyCharm


import json
import traceback

from flask import request

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from product.generate import Generate

logger = LoggerUtil().logger(__name__)


# 流水报告产品处理
class P08001(Generate):
    def __init__(self) -> None:
        super().__init__()
        self.response: {}

    def shake_hand_process(self):
        """
        json_data主体的关联关系
        需要根据关联关系，处理**portrait的相关数据
        """
        json_data = request.get_json()

    def strategy_process(self):
        try:
            json_data = request.get_json()
            logger.info("1. 流水报告：获取策略引擎结果，流程开启, 入参为：%s", json.dumps(json_data))

            logger.info("2. 流水报告，应答：%s", json.dumps(self.response))
        except Exception as err:
            logger.error(traceback.format_exc())
            raise ServerException(code=500, description=str(err))
