import pandas as pd
from pandas.tseries import offsets

from logger.logger_util import LoggerUtil
from mapping.module_processor import ModuleProcessor
from util.mysql_reader import sql_to_df

logger = LoggerUtil().logger(__name__)


class CreditInfo(ModuleProcessor):

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
            'credit_card_max_overdue_month_60m': 0,  # 5年内单张信用卡最大逾期期数
            'credit_guar_bal': 0  # 个人对外担保总负债余额
        }
        self.full_msg = None
        self.report_id = None
        self.report_time = None
        self.pcredit_loan = None
        self.pcredit_info = None
        self.pcredit_repayment = None
        self.pcredit_query = None

    def fetch_info(self, table_name):
        sql = """select * from %(table_name)s where report_id = %(report_id)s"""
        df = sql_to_df(sql=sql, params={'table_name': table_name,
                                        'report_id': self.report_id})
        return df

    def base_info(self, req_no):
        sql = """select report_id, report_time from credit_base_info where report_id = 
            (select report_id from credit_parse_request where biz_req_no = %(biz_req_no)s)"""
        base_df = sql_to_df(sql=sql, params={'biz_req_no': req_no})
        if base_df.shape[0] > 0:
            return
        self.report_id = base_df['report_id'].tolist()[0]
        self.report_time = pd.to_datetime(base_df['report_time'].tolist()[0])
        self.pcredit_loan = self.fetch_info('pcredit_loan')
        self.pcredit_info = self.fetch_info('pcredit_info')
        self.pcredit_repayment = self.fetch_info('pcredit_repayment')
        self.pcredit_query = self.fetch_info('pcredit_query_record')

    def credit_variables(self):
        if self.pcredit_loan is not None:
            loan_df = self.pcredit_loan[self.pcredit_loan['account_type'].isin(['01', '02', '03'])]
            credit_df = self.pcredit_loan[self.pcredit_loan['account_type'].isin(['04', '05'])]
            if loan_df.shape[0] > 0:
                self.variables['loan_amount_min_amt'] = \
                    0 if pd.isna(loan_df['loan_amount'].min()) else round(loan_df['loan_amount'].min(), 2)
                loan_amount_df = loan_df[
                    loan_df['loan_date'] >= self.report_time.date + offsets.DateOffset(months=-6)]
                self.variables['loan_cnt_inc_6m'] = loan_amount_df.shape[0]
                loan_amount_avg_6m = loan_amount_df['loan_amount'].mean()
                self.variables['loan_amount_avg_6m'] = loan_amount_avg_6m \
                    if pd.isna(loan_amount_avg_6m) else round(loan_amount_avg_6m, 2)
                total_bank_credit_limit = 0
                if self.pcredit_info is not None and self.pcredit_info.shape[0] > 0:
                    total_bank_credit_limit = round(self.pcredit_info.loc[0, ['non_revolloan_totalcredit',
                                                                              'revolcredit_totalcredit',
                                                                              'revolloan_totalcredit',
                                                                              'undestroy_limit',
                                                                              'undestory_semi_limit']].sum(), 2)
                    self.variables['credit_guar_bal'] = \
                        round(self.pcredit_info.loc[0, ['ind_repay_balance', 'ent_repay_balance']].sum(), 2)
                guar_type_1_4_amt = loan_df[loan_df['loan_guarantee_type'].isin(['01', '04'])]['loan_amount'].sum()
                self.variables['guar_type_1_4_amt_prop'] = round(guar_type_1_4_amt / total_bank_credit_limit, 2) \
                    if total_bank_credit_limit > 0 else 0
                guar_type_4_avg_amt = loan_df[loan_df['loan_guarantee_type'] == '04']['loan_amount'].mean()
                self.variables['guar_type_4_avg_amt'] = 0 if pd.isna(guar_type_4_avg_amt) else guar_type_4_avg_amt

                business_loan_df = loan_df[(loan_df['loan_type'].isin(['01', '07', '99'])) |
                                           ((loan_df['loan_type'] == '04') & (loan_df['loan_amount'] > 200000))]
                if business_loan_df.shape[0] > 0:
                    before_3m = self.report_time + offsets.DateOffset(months=-3)
                    before_6m = self.report_time + offsets.DateOffset(months=-6)
                    business_overdue_df = self.pcredit_repayment[
                        (self.pcredit_repayment['record_id'].isin(business_loan_df['id'].tolist())) &
                        ((self.pcredit_repayment['status'].str.isdigit()) |
                         (self.pcredit_repayment['repayment_amt'] > 0))]
                    self.variables['single_business_loan_overdue_cnt_3m'] = business_overdue_df[
                        ((self.pcredit_repayment['jhi_year'] > before_3m.year) |
                         ((self.pcredit_repayment['jhi_year'] == before_3m.year) &
                          (self.pcredit_repayment['month'] > before_3m.month)))]['record_id'].value_counts().max()
                    self.variables['single_business_loan_overdue_cnt_6m'] = business_overdue_df[
                        ((self.pcredit_repayment['jhi_year'] > before_6m.year) |
                         ((self.pcredit_repayment['jhi_year'] == before_6m.year) &
                          (self.pcredit_repayment['month'] > before_6m.month)))]['record_id'].value_counts().max()

            if credit_df.shape[0] > 0:
                before_5y = self.report_time + offsets.DateOffset(years=-5)
                credit_overdue_df = self.pcredit_repayment[
                    (self.pcredit_repayment['record_id'].isin(credit_df['id'].tolist())) &
                    (self.pcredit_repayment['repayment_amt'] > 1000) &
                    ((self.pcredit_repayment['jhi_year'] > before_5y.year) |
                     ((self.pcredit_repayment['jhi_year'] == before_5y.year) &
                      (self.pcredit_repayment['month'] > before_5y.month)))]
                self.variables['credit_min_payed_number'] = \
                    credit_df[credit_df['repay_amount'] * 2 > credit_df['amout_replay_amount']].shape[0]
                if credit_overdue_df.shape[0] > 0:
                    self.variables['credit_card_max_overdue_amt_60m'] = credit_overdue_df['repayment_amt'].max()
                    self.variables['credit_card_max_overdue_month_60m'] = \
                        credit_overdue_df['record_id'].value_counts().max()

        if self.pcredit_query is not None:
            before_3m = self.report_time + offsets.DateOffset(months=-3)
            self.variables['query_cnt_3m'] = self.pcredit_query[
                ((self.pcredit_query['reason'].isin(['01', '02', '08'])) |
                 (self.pcredit_query['reason'].str.contains('融资审批|贷款审批|信用卡审批|保前审查'))) &
                (self.pcredit_query['jhi_time'] >= before_3m.date)
            ].groupby(['operator', 'reason']).agg({'report_id': len}).shape[0]

    def process(self):
        per_msg = self.full_msg.get('riskSubject')
        if per_msg is None:
            return
        req_no = per_msg.get('creditParseReqNo')
        self.base_info(req_no)
        self.credit_variables()
