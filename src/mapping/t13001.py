import pandas as pd
from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def subtract_datetime_col(df, col_name1, col_name2, time_unit='M'):
    cols = df.columns
    if col_name1 in cols and col_name2 in cols:
        sub_name = col_name1 + '_' + col_name2 + time_unit
        df[col_name1] = pd.to_datetime(df[col_name1])
        df[col_name2] = pd.to_datetime(df[col_name2])
        df[sub_name] = df[col_name1] - df[col_name2]
        df[sub_name] = df[sub_name] / np.timedelta64(1, time_unit)
        return df[sub_name]
    else:
        return None

class T13001(Transformer):
    """
    短信核查相关的变量模块
    """

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'sms_reg_cnt': 0,  # 短信核查_注册总次数
            'sms_reg_cnt_bank_3m': 0,  # 短信核查_近3个月内银行类注册次数
            'sms_reg_cnt_other_3m': 0,  # 短信核查_近3个月内非银行类注册次数
            'sms_app_cnt': 0,  # 短信核查_申请总次数
            'sms_max_apply': 0,  # 短信核查_申请金额最大等级
            'sms_loan_cnt': 0,  # 短信核查_放款总次数
            'sms_max_loan': 0,  # 短信核查_放款金额最大等级
            'sms_reject_cnt': 0,  # 短信核查_驳回总次数
            'sms_overdue_cnt': 0,  # 短信核查_逾期总次数
            'sms_max_overdue': 0,  # 短信核查_逾期金额最大等级
            'sms_owe_cnt': 0,  # 短信核查_欠款总次数
            'sms_max_owe': 0,  # 短信核查_欠款金额最大等级
            'sms_owe_cnt_6m': 0,  # 短信核查_近6个月内欠款次数
            'sms_owe_cnt_6_12m': 0,  # 短信核查_近6-12个月内欠款次数
            'sms_max_owe_6m': 0,  # 短信核查_近6个月内欠款金额最大等级
        }


    ## 获取目标数据集1
    def _info_sms_loan_platform(self):

        sql1 = '''
            SELECT * FROM info_sms_loan_platform  WHERE sms_id 
            IN (SELECT sms.sms_id FROM (SELECT sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1) as sms);
        '''
        sql2 = '''
            SELECT sms_id,create_time FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
        '''
        df1 = sql_to_df(sql=(sql1),params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df2 = sql_to_df(sql=(sql2),params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df = pd.merge(df1, df2, how='left', on='sms_id')
        df['date_dif'] = subtract_datetime_col(df,'create_time','register_time','M')
        return df

    ## 计算短信核查_注册总次数
    def _sms_reg_cnt(self, df=None):

        self.variables['sms_reg_cnt'] = len(df)

    ## 计算短信核查_近3个月内银行类注册次数
    def _sms_reg_cnt_bank_3m(self, df=None):

        if len(df) != 0:
            df_1 = df.loc[(df['date_dif'] < 3) & (df['platform_type'] == 'BANK'), :].copy()
            self.variables['sms_reg_cnt_bank_3m'] = len(df_1)

    ## 计算短信核查_近3个月内非银行类注册次数
    def _sms_reg_cnt_other_3m(self, df=None):

        if len(df) != 0:
            df_2 = df.loc[(df['date_dif'] < 3) & (df['platform_type'] == 'NON_BANK'), :].copy()
            self.variables['sms_reg_cnt_other_3m'] = len(df_2)

    ## 获取目标数据集2
    def _info_sms_loan_apply(self):

        sql = '''
            SELECT * FROM info_sms_loan_apply WHERE sms_id 
            IN (SELECT sms.sms_id FROM (SELECT sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1) as sms);
        '''
        df = sql_to_df(sql=(sql),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_申请总次数
    def _sms_app_cnt(self, df=None):

        self.variables['sms_app_cnt'] = len(df)

    ## 计算短信核查_申请金额最大等级
    def _sms_max_apply(self, df=None):

        df['apply_amount'] = df['apply_amount'].replace(to_replace="0W～0.2W", value=1)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="0.2W～0.5W", value=2)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="0.5W～1W", value=3)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="1W～3W", value=4)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="3W～5W", value=5)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="5W～10W", value=6)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="10W以上", value=7)
        self.variables['sms_max_apply'] = df['apply_amount'].max()

    ## 获取目标数据集3
    def _info_sms_loan(self):

        sql = '''
            SELECT * FROM info_sms_loan WHERE sms_id 
            IN (SELECT sms.sms_id FROM (SELECT sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1) as sms);
        '''
        df = sql_to_df(sql=(sql),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_放款总次数
    def _sms_loan_cnt(self, df=None):

        self.variables['sms_loan_cnt'] = len(df)

    ## 计算短信核查_放款金额最大等级
    def _sms_max_loan(self, df=None):

        df['loan_amount'] = df['loan_amount'].replace(to_replace="0W～0.2W", value=1)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="0.2W～0.5W", value=2)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="0.5W～1W", value=3)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="1W～3W", value=4)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="3W～5W", value=5)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="5W～10W", value=6)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="10W以上", value=7)
        self.variables['sms_max_loan'] = df['loan_amount'].max()

    ## 获取目标数据集4
    def _info_sms_loan_reject(self):

        sql = '''
            SELECT * FROM info_sms_loan_reject WHERE sms_id 
            IN (SELECT sms.sms_id FROM (SELECT sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1) as sms);
        '''
        df = sql_to_df(sql=(sql),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_驳回总次数
    def _sms_reject_cnt(self, df=None):

        self.variables['sms_reject_cnt'] = len(df)

    ## 获取目标数据集5
    def _info_sms_overdue_platform(self):

        sql = '''
            SELECT * FROM info_sms_overdue_platform WHERE sms_id 
            IN (SELECT sms.sms_id FROM (SELECT sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1) as sms);
        '''
        df = sql_to_df(sql=(sql),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_逾期总次数
    def _sms_overdue_cnt(self, df=None):

        self.variables['sms_overdue_cnt'] = len(df)

    ## 计算短信核查_逾期金额最大等级
    def _sms_max_overdue(self, df=None):

        df['overdue_money'] = df['overdue_money'].replace(to_replace="0W～0.2W", value=1)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="0.2W～0.5W", value=2)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="0.5W～1W", value=3)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="1W～3W", value=4)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="3W～5W", value=5)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="5W～10W", value=6)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="10W以上", value=7)
        self.variables['sms_max_overdue'] = df['overdue_money'].max()

    ## 获取目标数据集6
    def _info_sms_debt(self):

        sql1 = '''
            SELECT * FROM info_sms_debt WHERE sms_id 
            IN (SELECT sms.sms_id FROM (SELECT sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
            ORDER BY id DESC LIMIT 1) as sms);
        '''
        sql2 = '''
             SELECT platform_code,overdue_time FROM info_sms_overdue_platform WHERE sms_id
             IN (SELECT sms.sms_id FROM (SELECT sms_id FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
             ORDER BY id DESC LIMIT 1) as sms);
        '''
        sql3 = '''
            SELECT sms_id,create_time FROM info_sms WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s AND phone = %(phone)s 
        '''
        df1 = sql_to_df(sql=(sql1),
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df2 = sql_to_df(sql=(sql2),
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df3 = sql_to_df(sql=(sql3),
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        merge1 = pd.merge(df1, df2, how='left', on='platform_code')
        df = pd.merge(merge1, df3, how='left', on='sms_id')
        df['date_dif'] = (df['create_time'] - df['overdue_time']).map(lambda x: x.days / 30)
        return df

    ## 计算短信核查_欠款总次数
    def _sms_owe_cnt(self, df=None):

        self.variables['sms_owe_cnt'] = len(df)

    ## 计算短信核查_欠款金额最大等级
    def _sms_max_owe(self, df=None):

        df['debt_money'] = df['debt_money'].replace(to_replace="0W～0.2W", value=1)
        df['debt_money'] = df['debt_money'].replace(to_replace="0.2W～0.5W", value=2)
        df['debt_money'] = df['debt_money'].replace(to_replace="0.5W～1W", value=3)
        df['debt_money'] = df['debt_money'].replace(to_replace="1W～3W", value=4)
        df['debt_money'] = df['debt_money'].replace(to_replace="3W～5W", value=5)
        df['debt_money'] = df['debt_money'].replace(to_replace="5W～10W", value=6)
        df['debt_money'] = df['debt_money'].replace(to_replace="10W以上", value=7)
        self.variables['sms_max_owe'] = df['debt_money'].max()

    ## 计算短信核查_近6个月内欠款次数
    def _sms_owe_cnt_6m(self, df=None):

        if len(df) != 0:
            df_1 = df.loc[df['date_dif'] < 6, :].copy()
            self.variables['sms_owe_cnt_6m'] = len(df_1)

    ## 计算短信核查_近6-12个月内欠款次数
    def _sms_owe_cnt_6_12m(self, df=None):

        if len(df) != 0:
            df_2 = df.loc[(df['date_dif'] >= 6) & (df['date_dif'] < 12), :].copy()
            self.variables['sms_owe_cnt_6_12m'] = len(df_2)

    ## 计算'sms_max_owe_6m': 0,  # 短信核查_近6个月内欠款金额最大等级
    def _sms_max_owe_6m(self, df=None):

        if len(df) != 0:
            df_3 = df.loc[df['date_dif'] < 6, :].copy()
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0W～0.2W", value=1)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0.2W～0.5W", value=2)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0.5W～1W", value=3)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="1W～3W", value=4)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="3W～5W", value=5)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="5W～10W", value=6)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="10W以上", value=7)
            self.variables['sms_max_owe_6m'] = df_3['debt_money'].max()

    ##  执行变量转换
    def transform(self):
        self._sms_reg_cnt(self._info_sms_loan_platform())
        self._sms_reg_cnt_bank_3m(self._info_sms_loan_platform())
        self._sms_reg_cnt_other_3m(self._info_sms_loan_platform())
        self._sms_app_cnt(self._info_sms_loan_apply())
        self._sms_max_apply(self._info_sms_loan_apply())
        self._sms_loan_cnt(self._info_sms_loan())
        self._sms_max_loan(self._info_sms_loan())
        self._sms_reject_cnt(self._info_sms_loan_reject())
        self._sms_overdue_cnt(self._info_sms_overdue_platform())
        self._sms_max_overdue(self._info_sms_overdue_platform())
        self._sms_owe_cnt(self._info_sms_debt())
        self._sms_max_owe(self._info_sms_debt())
        self._sms_owe_cnt_6m(self._info_sms_debt())
        self._sms_owe_cnt_6_12m(self._info_sms_debt())
        self._sms_max_owe_6m(self._info_sms_debt())
