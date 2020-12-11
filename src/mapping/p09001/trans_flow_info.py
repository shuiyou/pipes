import pandas as pd
from pandas.tseries import offsets

from logger.logger_util import LoggerUtil
from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df

logger = LoggerUtil().logger(__name__)


class TransFlowInfo(ModuleProcessor):

    def __init__(self):
        super().__init__()
        self.variables = {
            'operating_income_6m': 0,  # 6个月经营收入
            'balance_day_avg_6m': 0,  # 6个月余额日均
            'flow_limit_amt': 0,  # 流水指标
        }
        self.full_msg = None
        self.trans_u_flow = None

    def basic_data(self):
        app_no = self.full_msg.get('outApplyNo')
        if app_no is not None:
            sql = """select * from trans_u_flow_portrait where report_req_no = 
            (select report_req_no from trans_apply where app_no = %(app_no)s order by id desc limimt 1)"""
            self.trans_u_flow = sql_to_df(sql=sql, params={'app_no': app_no})

    def flow_variables(self):
        if self.trans_u_flow is not None and self.trans_u_flow.shape[0] > 0:
            self.trans_u_flow['trans_date'] = self.trans_u_flow['trans_time'].apply(lambda x: x.date)
            trans_max = self.trans_u_flow['trans_time'].max()
            # 剔除强关联关系及异常交易类型
            temp_trans_detail = self.trans_u_flow[
                (pd.isnull(self.trans_u_flow['relationship'])) &
                (pd.isnull(self.trans_u_flow['unusual_trans_type'])) &
                (self.trans_u_flow['trans_time'] >= trans_max + offsets.DateOffset(months=-6))]
            if temp_trans_detail.shape[0] > 0:
                operating_income_6m = temp_trans_detail[
                    temp_trans_detail['trans_amt'] > 0]['trans_amt'].sum()
                self.variables['operating_income_6m'] = operating_income_6m / 10000
                temp_trans_detail.drop_duplicates(subset='trans_date', keep='last', inplace=True)
                date_list = temp_trans_detail['trans_date'].tolist()
                amt_list = temp_trans_detail['account_balance'].tolist()
                diff_days = [(date_list[i + 1] - date_list[i]).days for i in range(len(date_list) - 1)]
                diff_days.append(1)
                total_days = sum(diff_days)
                total_amt_list = [diff_days[j] * amt_list[j] for j in range(len(date_list))]
                balance_day_avg_6m = sum(total_amt_list) / total_days if total_days != 0 else 0
                self.variables['balance_day_avg_6m'] = balance_day_avg_6m / 10000
                self.variables['flow_limit_amt'] = round(max(operating_income_6m / 15, balance_day_avg_6m * 5))

    def process(self):
        self.basic_data()
        self.flow_variables()
