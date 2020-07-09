import datetime

import pandas as pd

from util.mysql_reader import sql_to_df


def months_ago(end_date, months):
    end_year = end_date.year
    end_month = end_date.month
    end_day = end_date.day
    if end_month < months:
        res_month = 12 + end_month - months + 1
        res_year = end_year - 1
    else:
        res_month = end_month - months + 1
        res_year = end_year
    temp_date = datetime.datetime(res_year, res_month, 1) - datetime.timedelta(days=1)
    if temp_date.day <= end_day:
        return temp_date.date()
    else:
        return datetime.datetime(temp_date.year, temp_date.month, end_day).date()


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
        # 限制上传时间在3个月内的流水会生成画像表,后续可配置
        self.month_interval = 3
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

    def process(self):
        data = self.query_data_array[self.object_k]
        bank_account = None
        user_name = data.get('name')
        id_card_no = data.get('idno')
        if data.__contains__('extraParam') and data['extraParam'].__contains__('accounts') and \
                data['extraParam']['accounts'][self.object_k_k].__contains__('bankAccount'):
            bank_account = data['extraParam']['accounts'][self.object_k_k]['bankAccount']

        # 若关联人不存在银行卡号,则必然没有上传过流水,跳过此关联人
        if bank_account is None:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return
        sql = """select * from trans_flow where account_id in (select id from trans_account where account_name = '%s'
            and id_card_no = '%s' and account_no = '%s')""" % (user_name, id_card_no, bank_account)
        df = sql_to_df(sql)

        # 若数据库里面不存在该银行卡的流水信息,则跳过此关联人
        if len(df) == 0:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return

        # 最新流水上传时间必须在限制时间之内,暂定为3个月内,若在限定之间之外,则不重新生成画像表
        limit_time = pd.to_datetime(months_ago(datetime.datetime.now(), self.month_interval))
        if df['create_time'].max() < limit_time:
            self.account_id = None
            self.trans_flow_df = None
            self.trans_flow_portrait_df = None
            self.trans_flow_portrait_df_2_years = None
            return
        # 上述关系均没有跳过此关联人则正常走余下的流程
        self.trans_flow_df = self._time_interval(df, 2)
        self.account_id = self.trans_flow_df['account_id'].max()

    def trans_single_portrait(self):
        sql = """select * from trans_flow_portrait where account_id = '%s'""" % self.account_id
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

        if year != 1:
            if max_date.month == 12:
                years_before_first = datetime.datetime(max_date.year - year + 1, 1, 1)
            else:
                years_before_first = datetime.datetime(max_date.year - year, max_date.month + 1, 1)
        else:
            years_before_first = datetime.datetime(max_date.year - year, max_date.month, 1)
        min_date = min(min_date, years_before_first)
        flow_df = flow_df[(flow_df[filter_col] >= min_date) &
                          (flow_df[filter_col] <= max_date)]
        return flow_df
