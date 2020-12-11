# @Time : 12/11/20 9:57 AM 
# @Author : lixiaobo
# @File : micro_loan_amt_flow_executor.py 
# @Software: PyCharm
from logger.logger_util import LoggerUtil
from product.microloan.abs_micro_loan_flow import MicroLoanFlow

logger = LoggerUtil().logger(__name__)


class MicroLoanAmtFlowExecutor(MicroLoanFlow):
    def __init__(self, json_data):
        super().__init__(json_data)
        self.response = None

    def execute(self):
        pass
