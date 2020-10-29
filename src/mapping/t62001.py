import re

from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df
import pandas as pd


def get_left_field_value(df, key):
    df_temp = df[df['field_origin_name'] == key]
    if not df_temp.empty:
        value = df_temp['field_value'].to_list()[0]
        if pd.notna(value):
            if value == '-999' or value == '-1111':
                return 0
            else:
                value = re.findall(r"\d+\.?\d*", value)[0]
                return float(value)
        else:
            return 0
    else:
        return 0


def get_left_field_value1(df, key):
    df_temp = df[df['field_origin_name'] == key]
    if not df_temp.empty:
        value = df_temp['field_value'].to_list()[0]
        if pd.notna(value):
            if value == '-999' or value == '-1111':
                return float(value)
            else:
                value = re.findall(r"\d+\.?\d*", value)[0]
                return float(value)
        else:
            return 0
    else:
        return 0

def get_field_value(df, key):
    df_temp = df[df['field_origin_name'] == key]
    if not df_temp.empty:
        return df_temp['field_value'].to_list()[0]
    else:
        return None




class T62001(Transformer):
    """
    同盾模型
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'i_cnt_partner_Loan_finance_180day': 0,
            'i_cnt_partner_all_Imbank_90day': 0,
            'i_std_cnt_10daypartner_all_Imbank_90day': 0,
            'm_cnt_partner_all_finance_180day': 0,
            'm_cnt_partner_all_Imbank_90day': 0,
            'i_cnt_partner_all_P2pweb_90day': 0,
            'i_cnt_partner_all_Imbank_180day': None,
            'm_max_cnt_partner_daily_all_Unconsumerfinance_365day': None,
            'm_ratio_cnt_grp_id_Loan_all_all': None,
            'm_length_first_last_all_Imbank_60day': None,
            'i_length_first_last_all_Imbank_365day': None
        }

    def _info_risk_model_data(self):
        sql = '''
            select field_origin_name,field_value from info_risk_model_data where risk_model_id = (
            select id from info_risk_model where user_name = %(user_name)s and id_card_no = %(id_card_no)s
            and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 )
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        return df

    def clean_risk_model_data(self):
        df = self._info_risk_model_data()
        if df.empty:
            return
        self.variables['i_cnt_partner_Loan_finance_180day'] = get_left_field_value(df,
                                                                                   'i_cnt_partner_Loan_finance_180day')
        self.variables['i_cnt_partner_all_Imbank_90day'] = get_left_field_value(df,
                                                                                'i_cnt_partner_all_Imbank_90day')
        self.variables['i_std_cnt_10daypartner_all_Imbank_90day'] = get_left_field_value(df,
                                                                                         'i_std_cnt_10daypartner_all_Imbank_90day')
        self.variables['m_cnt_partner_all_finance_180day'] = get_left_field_value(df,
                                                                                  'm_cnt_partner_all_finance_180day')
        self.variables['m_cnt_partner_all_Imbank_90day'] = get_left_field_value(df,
                                                                                  'm_cnt_partner_all_Imbank_90day')
        self.variables['i_cnt_partner_all_P2pweb_90day'] = get_left_field_value(df,
                                                                                'i_cnt_partner_all_P2pweb_90day')
        self.variables['i_cnt_partner_all_Imbank_180day'] = get_left_field_value1(df,
                                                                                'i_cnt_partner_all_Imbank_180day')
        self.variables['m_max_cnt_partner_daily_all_Unconsumerfinance_365day'] = get_left_field_value1(df,
                                                                            'm_max_cnt_partner_daily_all_Unconsumerfinance_365day')
        self.variables['m_ratio_cnt_grp_id_Loan_all_all'] = get_left_field_value1(df,
                                                                            'field_origin_name=m_ratio_cnt_grp_id_Loan_all_all')
        self.variables['m_length_first_last_all_Imbank_60day'] = get_left_field_value1(df,
                                                                            'm_length_first_last_all_Imbank_60day')
        self.variables['i_length_first_last_all_Imbank_365day'] = get_left_field_value1(df,
                                                                                 'i_length_first_last_all_Imbank_365day')

    def transform(self):
        self.clean_risk_model_data()
