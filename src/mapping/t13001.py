import pandas as pd

from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class T13001(Transformer):
    """
    短信核查相关的变量模块
    """

    def __init__(self) -> None:

        super().__init__()
        self.variables = {
            'hd_reg_cnt': 0,  # 短信核查_注册总次数
            'hd_reg_cnt_bank_3m': 0,  # 短信核查_近3个月内银行类注册次数
            'hd_reg_cnt_other_3m': 0,  # 短信核查_近3个月内非银行类注册次数
            'hd_app_cnt': 0,  # 短信核查_申请总次数
            'hd_max_apply': 0,  # 短信核查_申请金额最大等级
            'hd_loan_cnt': 0,  # 短信核查_放款总次数
            'hd_max_loan': 0,  # 短信核查_放款金额最大等级
            'hd_reject_cnt': 0,  # 短信核查_驳回总次数
            'hd_overdue_cnt': 0,  # 短信核查_逾期总次数
            'hd_max_overdue': 0,  # 短信核查_逾期金额最大等级
            'hd_owe_cnt': 0,  # 短信核查_欠款总次数
            'hd_max_owe': 0,  # 短信核查_欠款金额最大等级
            'hd_owe_cnt_6m': 0,  # 短信核查_近6个月内欠款次数
            'hd_owe_cnt_6_12m': 0,  # 短信核查_近6-12个月内欠款次数
            'hd_max_owe_6m': 0,  # 短信核查_近6个月内欠款金额最大等级
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
        df1 = sql_to_df(sql=(sql1),
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df2 = sql_to_df(sql=(sql2),
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df = pd.merge(df1, df2, how='left', on='sms_id')
        df['date_dif'] = (df['create_time'] - df['register_time']).map(lambda x: x.days / 30)
        return df

    ## 计算短信核查_注册总次数
    def _hd_reg_cnt(self, df=None):

        self.variables['hd_reg_cnt'] = len(df)

    ## 计算短信核查_近3个月内银行类注册次数
    def _hd_reg_cnt_bank_3m(self, df=None):

        if len(df) != 0:
            df_1 = df.loc[(df['date_dif'] < 3) & (df['platform_type'] == 'BANK'), :].copy()
            self.variables['hd_reg_cnt_bank_3m'] = len(df_1)

    ## 计算短信核查_近3个月内非银行类注册次数
    def _hd_reg_cnt_other_3m(self, df=None):

        if len(df) != 0:
            df_2 = df.loc[(df['date_dif'] < 3) & (df['platform_type'] == 'NON_BANK'), :].copy()
            self.variables['hd_reg_cnt_other_3m'] = len(df_2)

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
    def _hd_app_cnt(self, df=None):

        self.variables['hd_app_cnt'] = len(df)

    ## 计算短信核查_申请金额最大等级
    def _hd_max_apply(self, df=None):

        df['apply_amount'] = df['apply_amount'].replace(to_replace="0W～0.2W", value=1)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="0.2W～0.5W", value=2)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="0.5W～1W", value=3)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="1W～3W", value=4)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="3W～5W", value=5)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="5W～10W", value=6)
        df['apply_amount'] = df['apply_amount'].replace(to_replace="10W以上", value=7)
        self.variables['hd_max_apply'] = df['apply_amount'].max()

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
    def _hd_loan_cnt(self, df=None):

        self.variables['hd_loan_cnt'] = len(df)

    ## 计算短信核查_放款金额最大等级
    def _hd_max_loan(self, df=None):

        df['loan_amount'] = df['loan_amount'].replace(to_replace="0W～0.2W", value=1)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="0.2W～0.5W", value=2)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="0.5W～1W", value=3)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="1W～3W", value=4)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="3W～5W", value=5)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="5W～10W", value=6)
        df['loan_amount'] = df['loan_amount'].replace(to_replace="10W以上", value=7)
        self.variables['hd_max_loan'] = df['loan_amount'].max()

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
    def _hd_reject_cnt(self, df=None):

        self.variables['hd_reject_cnt'] = len(df)

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
    def _hd_overdue_cnt(self, df=None):

        self.variables['hd_overdue_cnt'] = len(df)

    ## 计算短信核查_逾期金额最大等级
    def _hd_max_overdue(self, df=None):

        df['overdue_money'] = df['overdue_money'].replace(to_replace="0W～0.2W", value=1)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="0.2W～0.5W", value=2)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="0.5W～1W", value=3)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="1W～3W", value=4)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="3W～5W", value=5)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="5W～10W", value=6)
        df['overdue_money'] = df['overdue_money'].replace(to_replace="10W以上", value=7)
        self.variables['hd_max_overdue'] = df['overdue_money'].max()

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
    def _hd_owe_cnt(self, df=None):

        self.variables['hd_owe_cnt'] = len(df)

    ## 计算短信核查_欠款金额最大等级
    def _hd_max_owe(self, df=None):

        df['debt_money'] = df['debt_money'].replace(to_replace="0W～0.2W", value=1)
        df['debt_money'] = df['debt_money'].replace(to_replace="0.2W～0.5W", value=2)
        df['debt_money'] = df['debt_money'].replace(to_replace="0.5W～1W", value=3)
        df['debt_money'] = df['debt_money'].replace(to_replace="1W～3W", value=4)
        df['debt_money'] = df['debt_money'].replace(to_replace="3W～5W", value=5)
        df['debt_money'] = df['debt_money'].replace(to_replace="5W～10W", value=6)
        df['debt_money'] = df['debt_money'].replace(to_replace="10W以上", value=7)
        self.variables['hd_max_owe'] = df['debt_money'].max()

    ## 计算短信核查_近6个月内欠款次数
    def _hd_owe_cnt_6m(self, df=None):

        if len(df) != 0:
            df_1 = df.loc[df['date_dif'] < 6, :].copy()
            self.variables['hd_owe_cnt_6m'] = len(df_1)

    ## 计算短信核查_近6-12个月内欠款次数
    def _hd_owe_cnt_6_12m(self, df=None):

        if len(df) != 0:
            df_2 = df.loc[(df['date_dif'] >= 6) & (df['date_dif'] < 12), :].copy()
            self.variables['hd_owe_cnt_6_12m'] = len(df_2)

    ## 计算'hd_max_owe_6m': 0,  # 短信核查_近6个月内欠款金额最大等级
    def _hd_max_owe_6m(self, df=None):

        if len(df) != 0:
            df_3 = df.loc[df['date_dif'] < 6, :].copy()
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0W～0.2W", value=1)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0.2W～0.5W", value=2)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0.5W～1W", value=3)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="1W～3W", value=4)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="3W～5W", value=5)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="5W～10W", value=6)
            df_3['debt_money'] = df_3['debt_money'].replace(to_replace="10W以上", value=7)
            self.variables['hd_max_owe_6m'] = df_3['debt_money'].max()

    ##  执行变量转换
    def transform(self, user_name=None, id_card_no=None, phone=None):
        self.user_name = user_name
        self.id_card_no = id_card_no
        self.phone = phone

        self._hd_reg_cnt(self._info_sms_loan_platform())
        self._hd_reg_cnt_bank_3m(self._info_sms_loan_platform())
        self._hd_reg_cnt_other_3m(self._info_sms_loan_platform())
        self._hd_app_cnt(self._info_sms_loan_apply())
        self._hd_max_apply(self._info_sms_loan_apply())
        self._hd_loan_cnt(self._info_sms_loan())
        self._hd_max_loan(self._info_sms_loan())
        self._hd_reject_cnt(self._info_sms_loan_reject())
        self._hd_overdue_cnt(self._info_sms_overdue_platform())
        self._hd_max_overdue(self._info_sms_overdue_platform())
        self._hd_owe_cnt(self._info_sms_debt())
        self._hd_max_owe(self._info_sms_debt())
        self._hd_owe_cnt_6m(self._info_sms_debt())
        self._hd_owe_cnt_6_12m(self._info_sms_debt())
        self._hd_max_owe_6m(self._info_sms_debt())
