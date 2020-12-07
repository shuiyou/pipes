import pandas as pd

from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)


class CreditInfo(Transformer):

    def __init__(self):
        super().__init__()
        self.variables = {
            'loan_amount_min_amt': 0,  # 授信金额最小值
            'loan_amount_avg_6m': 0,  # 6个月内新增贷款金额笔均
            'guar_type_1_4_amt_prop': 0,  # 信用保证类贷款金额占比
            'single_business_loan_overdue_cnt_3m': 0,  # 3个月内单笔经营性贷款逾期次数
            'credit_min_payed_number': 0,  # 信用卡最低还款张数
            'guar_type_4_avg_amt': 0,  # 信用类贷款平均授信金额
            'credit_card_max_overdue_amt_60m': 0,  # 5年内信用卡最大逾期金额
            'single_business_loan_overdue_cnt_6m': 0,  # 6个月内单笔经营性贷款逾期次数
            'query_cnt_3m': 0,  # 3个月内贷款+贷记卡审批查询次数
            'loan_cnt_inc_6m': 0,  # 6个月内新增贷款笔数
            'credit_card_max_overdue_month_60m': 0  # 5年内单张信用卡最大逾期期数
        }
        self.full_msg = None

    def transform(self):
        pass
