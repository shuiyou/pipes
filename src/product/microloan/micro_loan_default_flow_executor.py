# @Time : 12/11/20 9:53 AM 
# @Author : lixiaobo
# @File : micro_loan_default_flow_executor.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
from product.microloan.abs_micro_loan_flow import MicroLoanFlow

logger = LoggerUtil().logger(__name__)


class MicroLoanDefaultFlowExecutor(MicroLoanFlow):
    def __init__(self, json_data):
        super().__init__(json_data)
        self.response = None

    def execute(self):
        pass



