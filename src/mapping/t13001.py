import datetime
import pandas as pd
from pandas.tseries import offsets

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


class T13001(Transformer):
    """
    短信核查相关的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'hd_regi_p2p_3m': 0,
            'hd_loan_non_bank_6m': 0,
            'hd_loan_month_std_24m': 0,
            'hd_apply_month_std_12m': 0,
            'hd_loan_month_std_12m': 0,
            'hd_regi_non_bank_3m': 0,
            'hd_loan_total_weight_amt_9m': 0,
            'regi_month_interval_max_2m': 0
        }
        self.two_years_ago = ''
        self.sms_id = ''
        self.end_date = None

    def _info_sms_id(self):
        id_no = self.id_card_no
        sql = """
            select sms_id, create_time
            from info_sms
            where id_card_no = %(id_no)s 
            and unix_timestamp(NOW()) < unix_timestamp(expired_at)
            order by id desc limit 1
        """
        df = sql_to_df(sql=sql,params={"id_no": str(id_no)})
        if df.shape[0] == 0:
            return '', None
        sms_id = str(df['sms_id'].to_list()[0])
        end_date = pd.to_datetime(df['create_time'].to_list()[0])
        return sms_id, end_date

    @staticmethod
    def datediff(data_df, date_col, end_date):
        if date_col not in data_df.columns:
            return data_df
        data_df[date_col] = data_df[date_col].apply(pd.to_datetime)
        data_df.sort_values(by=date_col, inplace=True)
        data_df.reset_index(drop=True, inplace=True)
        data_df['month_from_now'] = data_df[date_col].apply(
            lambda x: (end_date.year - x.year) * 12 + end_date.month - x.month + (end_date.day - x.day) // 100)
        data_df.loc[0, 'monthdiff'] = -1
        for i in data_df.index[1:]:
            data_df.loc[i, 'monthdiff'] = abs(data_df.loc[i, 'month_from_now'] - data_df.loc[i-1, 'month_from_now'])
        return data_df

    def _info_regi_data(self):
        sql = """
            select * 
            from info_sms_loan_platform
            where sms_id = %(sms_id)s and register_time > %(two_years_ago)s
        """
        df = sql_to_df(sql=sql,params={"sms_id": self.sms_id,
                                       "two_years_ago":self.two_years_ago})
        if df.shape[0] == 0:
            return
        df = self.datediff(df, 'register_time', self.end_date)
        regi_month_interval_max_2m = df[df['month_from_now'] < 2]['monthdiff'].max()
        self.variables['hd_regi_non_bank_3m'] = df[(df['month_from_now'] < 3) &
                                                   (df['platform_type'] == 'NON_BANK')].shape[0]
        self.variables['hd_regi_p2p_3m'] = df[(df['month_from_now'] < 3) &
                                              (df['platform_type'] == 'P2P')].shape[0]
        if pd.notna(regi_month_interval_max_2m):
            self.variables['regi_month_interval_max_2m'] = int(regi_month_interval_max_2m)

    def _info_apply_data(self):
        sql = """
            select *
            from info_sms_loan_apply
            where sms_id = %(sms_id)s and apply_time > %(two_years_ago)s
        """
        df = sql_to_df(sql=sql,params={"sms_id": self.sms_id,
                                       "two_years_ago":self.two_years_ago})
        if df.shape[0] == 0:
            return
        df = self.datediff(df, 'apply_time', self.end_date)
        hd_apply_month_std_12m = df[df['month_from_now'] < 12].groupby(
            'month_from_now').agg({'apply_time': len})['apply_time'].std()
        if pd.notna(hd_apply_month_std_12m):
            self.variables['hd_apply_month_std_12m'] = hd_apply_month_std_12m

    def _info_loan_data(self):
        sql = """
            select * 
            from info_sms_loan
            where sms_id = %(sms_id)s and loan_time > %(two_years_ago)s
        """
        df = sql_to_df(sql=sql,params={"sms_id": self.sms_id,
                                       "two_years_ago": self.two_years_ago})
        if df.shape[0] == 0:
            return
        df = self.datediff(df, 'loan_time', self.end_date)
        self.variables['hd_loan_non_bank_6m'] = df[(df['month_from_now'] < 6) &
                                                   (df['platform_type'] == 'NON_BANK')].shape[0]
        hd_loan_month_std_24m = df[df['month_from_now'] < 24].groupby(
            'month_from_now').agg({'loan_time': len})['loan_time'].std()
        hd_loan_month_std_12m = df[df['month_from_now'] < 12].groupby(
            'month_from_now').agg({'loan_time': len})['loan_time'].std()
        if pd.notna(hd_loan_month_std_12m):
            self.variables['hd_loan_month_std_12m'] = hd_loan_month_std_12m
        if pd.notna(hd_loan_month_std_24m):
            self.variables['hd_loan_month_std_24m'] = hd_loan_month_std_24m
        weight_mapping = {
            "0W～0.2W": 1000,
            "0.2W～0.5W": 3500,
            "0.5W～1W": 7500,
            "1W～3W": 20000,
            "3W～5W": 40000,
            "5W～10W": 75000,
            "10W以上": 100000
        }
        df['weight_amt'] = df['loan_amount'].map(weight_mapping)
        self.variables['hd_loan_total_weight_amt_9m'] = df[df['month_from_now'] < 9]['weight_amt'].sum()

    def transform(self):
        self.sms_id, self.end_date = self._info_sms_id()
        if self.end_date is None:
            return
        two_years_ago = self.end_date - offsets.DateOffset(years=2)
        self.two_years_ago = two_years_ago.strftime('%Y-%m-%d')
        self._info_apply_data()
        self._info_regi_data()
        self._info_loan_data()
