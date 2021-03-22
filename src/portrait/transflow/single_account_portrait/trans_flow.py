import pandas as pd
# TODO eval 动态导入，不能删除下面的导入。
from portrait.transflow.single_account_portrait.models import TransAccount, TransFlow, TransFlowPortrait, \
    TransSinglePortrait, TransSingleSummaryPortrait, TransSingleRemarkPortrait, TransSingleCounterpartyPortrait, \
    TransSingleRelatedPortrait, TransSingleLoanPortrait, TransApply, TransUFlowPortrait, TransULoanPortrait, \
    TransUModelling, TransUPortrait, TransUCounterpartyPortrait, TransURelatedPortrait, TransURemarkPortrait, \
    TransUSummaryPortrait, TransFlowException
from util.mysql_reader import sql_to_df
from pandas.tseries import offsets


def months_ago(end_date, months):
    return (pd.to_datetime(end_date) - offsets.DateOffset(months=months)).date()


def transform_class_str(params, class_name):
    func_str = class_name + '('
    for k, v in params.items():
        if v is not None and v != '':
            func_str += k + "='" + str(v) + "',"
    func_str = func_str[:-1]
    func_str += ')'
    value = eval(func_str)
    return value


class TransFlowBasic:

    def __init__(self, portrait):
        super().__init__()
        self.trans_flow_df = None
        self.account_id = None
        # # 限制上传时间在3个月内的流水会生成画像表,后续可配置
        # self.month_interval = 3
        self.object_k = 0
        self.object_nums = len(portrait.query_data_array)
        self.object_k_k = 0
        self.user_name = portrait.user_name
        self.query_data_array = portrait.query_data_array
        self.report_req_no = portrait.public_param.get('reportReqNo')
        self.app_no = portrait.public_param.get('outApplyNo')
        self.trans_flow_portrait_df = None
        self.trans_flow_portrait_df_2_years = None
        self.trans_u_flow_df = None
        self.trans_u_flow_portrait_df = None
        self.trans_u_flow_portrait_df_2_years = None
        self.db = portrait.sql_db
        self.user_type = None
        self.accounts_list = []

    def trans_accounts_list(self):
        data = self.query_data_array[self.object_k]
        # bank_account = None
        user_name = data.get('name')
        id_card_no = data.get('idno')
        self.user_type = data.get('userType')
        # 若为担保人，跳过
        if data.get('relation') == 'GUARANTOR':
            return
        sql = "select * from trans_account where account_name = '%s' and id_card_no = '%s'" % (user_name, id_card_no)
        df = sql_to_df(sql)
        if df.shape[0] > 0:
            accounts_list = df['account_no'].unique().tolist()
        else:
            accounts_list = []
        return accounts_list

    def process(self):
        data = self.query_data_array[self.object_k]
        # bank_account = None
        user_name = data.get('name')
        id_card_no = data.get('idno')
        self.user_type = data.get('userType')
        # 若为担保人，跳过
        if data.get('relation') == 'GUARANTOR':
            return
        # 若self.object_k_k超过self.accounts_list的下标限制，则跳过
        if self.object_k_k >= len(self.accounts_list):
            return

        sql = """select * from trans_flow where account_id in (select id from trans_account where account_name = '%s'
            and id_card_no = '%s' and account_no = '%s')""" % (user_name, id_card_no,
                                                               self.accounts_list[self.object_k_k])
        df = sql_to_df(sql)

        # 若数据库里面不存在该主体的流水信息,则跳过此关联人
        if len(df) == 0:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return
        # 上述关系均没有跳过此关联人则正常走余下的流程
        self.trans_flow_df = self._time_interval(df, 2)
        self.account_id = self.trans_flow_df['account_id'].max()

    def trans_single_portrait(self):
        sql = """select * from trans_flow_portrait where account_id = '%s' and report_req_no = '%s'""" \
              % (self.account_id, self.report_req_no)
        df = sql_to_df(sql)
        if len(df) == 0:
            return
        self.trans_flow_portrait_df = self._time_interval(df, 1)
        self.trans_flow_portrait_df_2_years = self._time_interval(df, 2)

    def u_process(self):
        sql = """select a.*,b.bank,b.account_no from trans_flow_portrait a left join trans_account b on 
            a.account_id=b.id where a.report_req_no = '%s'""" % self.report_req_no
        df = sql_to_df(sql)
        if len(df) == 0:
            return
        self.trans_u_flow_df = df

    def trans_union_portrait(self):
        sql = """select * from trans_u_flow_portrait where report_req_no = '%s'""" % self.report_req_no
        df = sql_to_df(sql)
        if len(df) == 0:
            return
        self.trans_u_flow_portrait_df = self._time_interval(df, 1)
        self.trans_u_flow_portrait_df_2_years = self._time_interval(df, 2)

    @staticmethod
    def _time_interval(df, year=1):
        flow_df = df.copy()
        if 'trans_date' in flow_df.columns:
            filter_col = 'trans_date'
        else:
            filter_col = 'trans_time'
        flow_df[filter_col] = pd.to_datetime(flow_df[filter_col])
        max_date = flow_df[filter_col].max()
        min_date = flow_df[filter_col].min()
        years_before_first = pd.to_datetime(max_date) - offsets.DateOffset(months=year*12)
        min_date = max(min_date, years_before_first)
        flow_df = flow_df[(flow_df[filter_col] >= min_date) &
                          (flow_df[filter_col] <= max_date)]
        return flow_df
