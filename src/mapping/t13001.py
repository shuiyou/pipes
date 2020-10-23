import pandas as pd
import numpy as np

from mapping.tranformer import Transformer, subtract_datetime_col
from util.mysql_reader import sql_to_df


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def month_clac(x):
    for i in range(25):
        days = (pd.datetime.now() - pd.DateOffset(months=i)) - x
        if days.days <= 0:
            return i

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
            'sms_app_cnt_3m': 0,  # 短信核查_近三个月内申请总次数
            'sms_loan_cnt_3m': 0,  # 短信核查_近三个月内放款总次数
            'hd_regi_p2p_3m': 0,
            'hd_loan_non_bank_6m': 0,
            'hd_loan_month_std_24m': 0,
            'hd_apply_month_std_12m': 0,
            'hd_loan_month_std_12m': 0,
            'hd_regi_non_bank_3m': 0,
            'hd_loan_total_weight_amt_9m': 0
        }

    ## 获取目标数据集1
    def _info_sms_loan_platform(self):

        sql1 = '''
            SELECT sms_id,platform_type,register_time 
            FROM info_sms_loan_platform  
            WHERE sms_id 
            IN (
                SELECT sms.sms_id 
                FROM (
                    SELECT sms_id 
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) as sms
            );
        '''
        sql2 = '''
            SELECT sms_id,create_time 
            FROM info_sms 
            WHERE 
                user_name = %(user_name)s 
                AND id_card_no = %(id_card_no)s 
                AND phone = %(phone)s 
                AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
        '''
        df1 = sql_to_df(sql=sql1,
                        params={"user_name": self.user_name,
                                "id_card_no": self.id_card_no,
                                "phone": self.phone})
        df2 = sql_to_df(sql=sql2,
                        params={"user_name": self.user_name,
                                "id_card_no": self.id_card_no,
                                "phone": self.phone})
        df = pd.merge(df1, df2, how='left', on='sms_id')
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'register_time', 'M')]
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
            SELECT sms_id, apply_amount
            FROM info_sms_loan_apply
            WHERE sms_id 
            IN (
                SELECT sms.sms_id
                FROM (
                    SELECT sms_id
                    FROM info_sms
                    WHERE 
                        user_name = %(user_name)s
                        AND id_card_no = %(id_card_no)s
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC
                    LIMIT 1
                ) sms
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_申请总次数
    def _sms_app_cnt(self, df=None):

        self.variables['sms_app_cnt'] = len(df)

    ## 计算短信核查_申请金额最大等级
    def _sms_max_apply(self, df=None):
        if len(df) != 0:
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
            SELECT sms_id,loan_amount 
            FROM info_sms_loan 
            WHERE sms_id 
            IN (
                SELECT sms.sms_id 
                FROM (
                    SELECT sms_id 
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) as sms
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_放款总次数
    def _sms_loan_cnt(self, df=None):

        self.variables['sms_loan_cnt'] = len(df)

    ## 计算短信核查_放款金额最大等级
    def _sms_max_loan(self, df=None):
        if len(df) != 0:
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
            SELECT sms_id 
            FROM info_sms_loan_reject 
            WHERE sms_id 
            IN (
                SELECT sms.sms_id 
                FROM (
                    SELECT sms_id 
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) as sms
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_驳回总次数
    def _sms_reject_cnt(self, df=None):

        self.variables['sms_reject_cnt'] = len(df)

    ## 获取目标数据集5
    def _info_sms_overdue_platform(self):

        sql = '''
            SELECT sms_id,overdue_money 
            FROM info_sms_overdue_platform 
            WHERE sms_id 
            IN (
                SELECT sms.sms_id 
                FROM (
                    SELECT sms_id 
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) as sms
            );
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        return df

    ## 计算短信核查_逾期总次数
    def _sms_overdue_cnt(self, df=None):

        self.variables['sms_overdue_cnt'] = len(df)

    ## 计算短信核查_逾期金额最大等级
    def _sms_max_overdue(self, df=None):
        if len(df) != 0:
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
            SELECT sms_id,platform_code,debt_money 
            FROM info_sms_debt 
            WHERE sms_id 
            IN (
                SELECT sms.sms_id 
                FROM (
                    SELECT sms_id 
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s 
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                ) as sms
            );
        '''
        sql2 = '''
             SELECT platform_code,overdue_time 
             FROM info_sms_overdue_platform 
             WHERE sms_id
             IN (
                 SELECT sms.sms_id 
                 FROM (
                     SELECT sms_id 
                     FROM info_sms 
                     WHERE 
                         user_name = %(user_name)s 
                         AND id_card_no = %(id_card_no)s 
                         AND phone = %(phone)s 
                         AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                     ORDER BY id DESC 
                     LIMIT 1
                 ) as sms
             );
        '''
        sql3 = '''
            SELECT sms_id,create_time 
            FROM info_sms 
            WHERE 
                user_name = %(user_name)s 
                AND id_card_no = %(id_card_no)s 
                AND phone = %(phone)s 
                AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
        '''
        df1 = sql_to_df(sql=sql1,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df2 = sql_to_df(sql=sql2,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        df3 = sql_to_df(sql=sql3,
                        params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        merge1 = pd.merge(df1, df2, how='left', on='platform_code')
        df = pd.merge(merge1, df3, how='left', on='sms_id')
        df['date_dif'] = df[subtract_datetime_col(df, 'create_time', 'overdue_time', 'M')]
        return df

    ## 计算短信核查_欠款总次数
    def _sms_owe_cnt(self, df=None):
        self.variables['sms_owe_cnt'] = len(df)

    ## 计算短信核查_欠款金额最大等级
    def _sms_max_owe(self, df=None):
        if len(df) != 0:
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

    ## 计算短信核查_近6个月内欠款金额最大等级
    def _sms_max_owe_6m(self, df=None):

        if len(df) != 0:
            df_3 = df.loc[df['date_dif'] < 6, :].copy()
            if len(df_3) != 0:
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0W～0.2W", value=1)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0.2W～0.5W", value=2)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="0.5W～1W", value=3)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="1W～3W", value=4)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="3W～5W", value=5)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="5W～10W", value=6)
                df_3['debt_money'] = df_3['debt_money'].replace(to_replace="10W以上", value=7)
                self.variables['sms_max_owe_6m'] = df_3['debt_money'].max()

    def _sms_app_cnt_3m(self):
        sql = '''
                select count(*) total_count from(
                    SELECT sms_id, create_time
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                    ) sms left join info_sms_loan_apply slp on slp.sms_id = sms.sms_id 
                        where apply_time >= DATE_ADD(sms.create_time, Interval -3 month)
                '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        if df is None or df.empty:
            return

        self.variables["sms_app_cnt_3m"] = df.iloc[0].total_count

    def _sms_loan_cnt_3m(self):
        sql = '''
                select count(*) total_count from(
                    SELECT sms_id, create_time
                    FROM info_sms 
                    WHERE 
                        user_name = %(user_name)s 
                        AND id_card_no = %(id_card_no)s 
                        AND phone = %(phone)s
                        AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
                    ORDER BY id DESC 
                    LIMIT 1
                    ) sms left join info_sms_loan sla on sla.sms_id = sms.sms_id 
                        where loan_time >= DATE_ADD(sms.create_time, Interval -3 month)
                '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no, "phone": self.phone})
        if df is None or df.empty:
            return

        self.variables["sms_loan_cnt_3m"] = df.iloc[0].total_count

    # 读取华道数据 info_sms 读取sms_id数据主键
    def _load_info_sms_id(self, dict_in) -> int:
        sql = """
           SELECT *
           FROM info_sms WHERE user_name = %(name)s
           and id_card_no = %(idno)s
           and unix_timestamp(NOW()) < unix_timestamp(expired_at);
        """
        df = sql_to_df(sql=sql, params={"name": dict_in['name'], "idno": dict_in['idno']})
        if df is not None and len(df) > 0:
            df.sort_values(by=['expired_at'], ascending=False, inplace=True)
            return int(df['sms_id'].iloc[0])

    # 读取 info_sms_loan 数据
    def _load_info_sms_loan_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT *
               FROM info_sms_loan
               WHERE sms_id = %(id)s;
        """
        info_sms_loan_df = sql_to_df(sql=sql, params={"id": id})
        if info_sms_loan_df is not None and len(info_sms_loan_df) > 0:
            return info_sms_loan_df
        return None

    # 计算 info_sms_loan 相关字段
    def _info_sms_loan1(self, df=None):
        if df is not None and len(df) > 0:
            df1 = df[
                (df['platform_type'] == "NON_BANK") & (
                            df['loan_time'] > pd.datetime.now() - pd.DateOffset(months=6))]
            self.variables['hd_loan_non_bank_6m'] += df1.shape[0]

            df2 = df[(df['loan_time'] > pd.datetime.now() - pd.DateOffset(months=24))]
            df2['month'] = df2['loan_time'].map(month_clac())
            self.variables['hd_loan_month_std_24m'] = np.round(np.std(df2.groupby(by='month').id.count()), 4)

            df3 = df[(df['loan_time'] > pd.datetime.now() - pd.DateOffset(months=12))]
            df3['month'] = df3['loan_time'].map(month_clac())
            self.variables['hd_loan_month_std_12m'] = np.round(np.std(df3.groupby(by='month').id.count()), 4)

            df4 = df[(df['loan_time'] > pd.datetime.now() - pd.DateOffset(months=9))]
            reset_dir = {
                '0W～0.2W': 1000,
                '0.2W～0.5W': 3500,
                '0.5W～1W': 7500,
                '1W～3W': 20000,
                '3W～5W': 40000,
                '5W～10W': 75000,
                '10W以上': 100000
            }
            df4['loan_amount'] = df['loan_amount'].map(reset_dir)

            self.variables['fin_mort_name'] += df['loan_amount'].sum()

    # 读取 info_sms_loan_platform 数据
    def _load_info_sms_loan_platform_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT *
               FROM info_sms_loan_platform
               WHERE sms_id = %(id)s;
        """
        info_sms_loan_platform_df = sql_to_df(sql=sql, params={"id": id})
        if info_sms_loan_platform_df is not None and len(info_sms_loan_platform_df) > 0:
            return info_sms_loan_platform_df
        return None

    # 计算 info_sms_loan_platform 相关字段
    def _info_platform(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['hd_regi_p2p_3m'] = len(df[(df['register_time'] > pd.datetime.now() - pd.DateOffset(
                months=3)) & (df['platform_type'] == "P2P")])
            self.variables['hd_regi_non_bank_3m'] = len(
                df[(df['register_time'] > pd.datetime.now() - pd.DateOffset(months=3)) & (
                            df['platform_type'] == "NON_BANK")])

    # 读取 info_sms_loan_apply 数据
    def _load_info_sms_loan_apply_df(self, id) -> pd.DataFrame:
        sql = """
               SELECT *
               FROM info_sms_loan_apply
               WHERE sms_id = %(id)s;
        """
        info_sms_loan_apply_df = sql_to_df(sql=sql, params={"id": id})
        if info_sms_loan_apply_df is not None and len(info_sms_loan_apply_df) > 0:
            return info_sms_loan_apply_df
        return None

    # 计算 info_sms_loan_apply 相关字段
    def _info_apply(self, df=None):
        if df is not None and len(df) > 0:
            df2 = df[(df['loan_time'] > pd.datetime.now() - pd.DateOffset(months=12))]
            df2['month'] = df2['loan_time'].map(month_clac())
            self.variables['hd_apply_month_std_12m'] = np.round(np.std(df2.groupby(by='month').id.count()), 4)

    ##  执行变量转换
    def transform(self):
        platform_df = self._info_sms_loan_platform()
        self._sms_reg_cnt(platform_df)
        self._sms_reg_cnt_bank_3m(platform_df)
        self._sms_reg_cnt_other_3m(platform_df)
        apply = self._info_sms_loan_apply()
        self._sms_app_cnt(apply)
        self._sms_max_apply(apply)
        loan = self._info_sms_loan()
        self._sms_loan_cnt(loan)
        self._sms_max_loan(loan)
        self._sms_reject_cnt(self._info_sms_loan_reject())
        platform = self._info_sms_overdue_platform()
        self._sms_overdue_cnt(platform)
        self._sms_max_overdue(platform)
        debt = self._info_sms_debt()
        self._sms_owe_cnt(debt)
        self._sms_max_owe(debt)
        self._sms_owe_cnt_6m(debt)
        self._sms_owe_cnt_6_12m(debt)
        self._sms_max_owe_6m(debt)
        self._sms_app_cnt_3m()
        self._sms_loan_cnt_3m()

        each = self.origin_data
        sms_id = self._load_info_sms_id(each)

        df = self._load_info_sms_loan_df(sms_id)
        self._info_sms_loan1(df)

        df = self._load_info_sms_loan_platform_df(sms_id)
        self._info_platform(df)

        df = self._load_info_sms_loan_apply_df(sms_id)
        self._info_apply(df)


