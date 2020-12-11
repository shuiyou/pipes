# @Time : 12/11/20 9:58 AM 
# @Author : lixiaobo
# @File : micro_loan_common_flow_executor.py 
# @Software: PyCharm
import json

from logger.logger_util import LoggerUtil
from mapping.grouped_tranformer import invoke_union
from mapping.utils.np_encoder import NpEncoder
from product.microloan.abs_micro_loan_flow import MicroLoanFlow
from product.p_utils import score_to_int
from util.strategy_invoker import invoke_strategy
from util.type_converter import format_var
from view.grouped_mapper_detail import view_variables_scheduler

logger = LoggerUtil().logger(__name__)


class MicroLoanCommonFlowExecutor(MicroLoanFlow):
    def __init__(self, json_data):
        super().__init__(json_data)
        self.response = None

    def execute(self):
        subject = []
        cache_array = []

        # 遍历query_data_array调用strategy
        for data in self.query_data_array:
            array, resp = self._strategy_hand(self.json_data, data, self.product_code, self.req_no)
            subject.append(resp)
            cache_array.append(array)
            # 最后返回报告详情
        common_detail = view_variables_scheduler(self.product_code, self.json_data,
                                                 None, None, None, None, None, None, invoke_union)

        # 封装第二次调用参数
        variables = self._create_strategy_second_request(cache_array)

        strategy_resp = invoke_strategy(variables, self.product_code, self.req_no)
        score_to_int(strategy_resp)
        # 封装最终返回json
        resp_end = self._create_strategy_resp(strategy_resp, variables, common_detail, subject, self.json_data)
        format_var(None, None, -1, resp_end)
        logger.info("response:%s", json.dumps(resp_end, cls=NpEncoder))
        self.response = resp_end
