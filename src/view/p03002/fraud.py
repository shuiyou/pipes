import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer, invoke_each
from util.mysql_reader import sql_to_df


def cts_match(df, params):
    return df[df['field_name'].isin(params)]


def get_factor_field_value(df, key):
    df_temp = df[df['field_name'] == key]
    if not df_temp.empty:
        return float(df_temp['field_value'].to_list()[0])
    else:
        return 0


class Fraud(GroupedTransformer):

    def group_name(self):
        return "fraud"

    def invoke_style(self) -> int:
        return invoke_each

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'fraud_trace_cnt': 0,
            'fraud_wifi_cnt': 0,
            'fraud_act_cnt': 0,
            'fraud_app_cnt': 0,
            'fraud_loan_app_cnt': 0,
            'fraud_msg_cnt': 0,
            'fraud_trace_city_180_cnt': 0,
            'fraud_trace_city_30_cnt': 0,
            'fraud_trace_city_15_cnt': 0,
            'fraud_trace_wifi_15_cnt': 0,
            'fraud_trace_wifi_60_cnt': 0,
            'fraud_trace_wifi_90_cnt': 0,
            'fraud_trace_wifi_9_cnt': 0,
            'fraud_trace_act_day_avg_area': 0.5,
            'fraud_trace_act_night_avg_area': 0.5,
            'fraud_trace_act_day_area': 0,
            'fraud_trace_act_night_area': 0,
            'fraud_bank_app_0_30_score': 0,
            'fraud_bank_app_30_60_score': 0,
            'fraud_bank_app_60_90_score': 0,
            'fraud_loan_app_0_30_score': 0,
            'fraud_loan_app_30_60_score': 0,
            'fraud_loan_app_60_90_score': 0,
            'fraud_pay_app_0_30_score': 0,
            'fraud_pay_app_30_60_score': 0,
            'fraud_pay_app_60_90_score': 0,
            'fraud_fintool_app_0_30_score': 0,
            'fraud_fintool_app_30_60_score': 0,
            'fraud_fintool_app_60_90_score': 0,
            'fraud_invest_app_0_30_score': 0,
            'fraud_invest_app_30_60_score': 0,
            'fraud_invest_app_60_90_score': 0,
            'fraud_bank_app_avg_score': 5.4,
            'fraud_loan_app_avg_score': 1.6,
            'fraud_pay_app_avg_score': 1.7,
            'fraud_fintool_app_avg_score': 0.5,
            'fraud_invest_app_avg_score': 1.2,
            'fraud_loan_app_unistall_180_ratio': 0,
            'fraud_loan_app_unistall_90_ratio': 0,
            'fraud_loan_app_unistall_60_ratio': 0,
            'fraud_loan_app_unistall_30_ratio': 0,
            'fraud_loan_app_unistall_15_ratio': 0,
            'fraud_fin_app_unistall_180_ratio': 0,
            'fraud_fin_app_unistall_90_ratio': 0,
            'fraud_fin_app_unistall_60_ratio': 0,
            'fraud_fin_app_unistall_30_ratio': 0,
            'fraud_fin_app_unistall_15_ratio': 0,
            'fraud_loan_app_unistall_180_avg_ratio': 0.289,
            'fraud_loan_app_unistall_90_avg_ratio': 0.273,
            'fraud_loan_app_unistall_60_avg_ratio': 0.255,
            'fraud_loan_app_unistall_30_avg_ratio': 0.212,
            'fraud_loan_app_unistall_15_avg_ratio': 0.112,
            'fraud_fin_app_unistall_180_avg_ratio': 0.221,
            'fraud_fin_app_unistall_90_avg_ratio': 0.238,
            'fraud_fin_app_unistall_60_avg_ratio': 0.23,
            'fraud_fin_app_unistall_30_avg_ratio': 0.19,
            'fraud_fin_app_unistall_15_avg_ratio': 0.153,
            'fraud_msg_fin_1_cnt': 0,
            'fraud_msg_loan_3_cnt': 0,
            'fraud_msg_fin_9_cnt': 0,
            'fraud_msg_loan_9_cnt': 0
        }

    def _info_risk_cts_item(self):
        sql = '''
            select * from info_risk_cts_item where risk_cts_id = (
            select id from info_risk_cts where mobile= %(mobile)s AND unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1 )
        '''
        df = sql_to_df(sql=sql,
                       params={"mobile": self.phone})
        return df

    def _info_risk_factor_item(self):
        sql = '''
                    select field_name,field_value from info_risk_factor_item where risk_factor_id = (
                        select id from info_risk_factor where mobile = %(mobile)s 
                        and unix_timestamp(NOW()) < unix_timestamp(expired_at) order by id desc limit 1
                    )
                '''
        df = sql_to_df(sql=sql,
                       params={"mobile": self.phone})
        return df

    def clean_variables_factor(self):
        df = self._info_risk_factor_item()
        if df.empty:
            return
        self.variables['fraud_bank_app_0_30_score'] = get_factor_field_value(df, 'FIN_Bank_all_0M_1M')
        self.variables['fraud_bank_app_30_60_score'] = get_factor_field_value(df, 'FIN_Bank_all_1M_2M')
        self.variables['fraud_bank_app_60_90_score'] = get_factor_field_value(df, 'FIN_Bank_all_2M_3M')
        self.variables['fraud_loan_app_0_30_score'] = get_factor_field_value(df, 'FIN_Loan_all_0M_1M')
        self.variables['fraud_loan_app_30_60_score'] = get_factor_field_value(df, 'FIN_Loan_all_1M_2M')
        self.variables['fraud_loan_app_60_90_score'] = get_factor_field_value(df, 'FIN_Loan_all_2M_3M')
        self.variables['fraud_pay_app_0_30_score'] = get_factor_field_value(df, 'FIN_Bank_all_0M_1M')
        self.variables['fraud_pay_app_30_60_score'] = get_factor_field_value(df, 'FIN_Bank_all_1M_2M')
        self.variables['fraud_pay_app_60_90_score'] = get_factor_field_value(df, 'FIN_Bank_all_2M_3M')
        self.variables['fraud_fintool_app_0_30_score'] = get_factor_field_value(df, 'FIN_Payment_all_0M_1M')
        self.variables['fraud_fintool_app_30_60_score'] = get_factor_field_value(df, 'FIN_Payment_all_1M_2M')
        self.variables['fraud_fintool_app_60_90_score'] = get_factor_field_value(df, 'FIN_Payment_all_2M_3M')
        self.variables['fraud_invest_app_0_30_score'] = get_factor_field_value(df, 'FIN_Investing_all_0M_1M')
        self.variables['fraud_invest_app_30_60_score'] = get_factor_field_value(df, 'FIN_Investing_all_1M_2M')
        self.variables['fraud_invest_app_60_90_score'] = get_factor_field_value(df, 'FIN_Investing_all_2M_3M')
        self.variables['fraud_bank_credit_card_30d_score'] = get_factor_field_value(df, 'FIN_Bank_Creditcard_all_0M_1M')
        self.variables['fraud_bank_internet_30d_score'] = get_factor_field_value(df, 'FIN_Bank_Type_Internet_all_0M_1M')
        self.variables['fraud_bank_sharehold_30d_score'] = get_factor_field_value(df, 'FIN_Bank_Type_Sharehold_all_0M_1M')
        self.variables['fraud_bank_state_30d_score'] = get_factor_field_value(df,
                                                                                  'FIN_Bank_Type_StateCommercial_all_0M_1M')
        self.variables['fraud_bank_rural_30d_score'] = get_factor_field_value(df,
                                                                              'FIN_Bank_Type_Rural_all_0M_1M')
        self.variables['fraud_bank_urban_30d_score'] = get_factor_field_value(df,
                                                                              'FIN_Bank_Type_Urban_all_0M_1M')
        self.variables['fraud_loan_car_30d_score'] = get_factor_field_value(df,
                                                                              'FIN_Loan_Car_all_0M_1M')
        self.variables['fraud_loan_cash_30d_score'] = get_factor_field_value(df,
                                                                            'FIN_Loan_Cash_all_0M_1M')
        self.variables['fraud_loan_consumer_30d_score'] = get_factor_field_value(df,
                                                                             'FIN_Loan_Consumer_all_0M_1M')
        self.variables['fraud_loan_credit_card_30d_score'] = get_factor_field_value(df,
                                                                                 'FIN_Loan_CreditCard_all_0M_1M')
        self.variables['fraud_loan_house_30d_score'] = get_factor_field_value(df,
                                                                                    'FIN_Loan_House_all_0M_1M')
        self.variables['fraud_loan_mortgage_30d_score'] = get_factor_field_value(df,
                                                                              'FIN_Loan_Mortgage_all_0M_1M')
        self.variables['fraud_loan_other_30d_score'] = get_factor_field_value(df,
                                                                                 'FIN_Loan_Other_all_0M_1M')
        self.variables['fraud_loan_p2p_30d_score'] = get_factor_field_value(df,
                                                                              'FIN_Loan_P2P_all_0M_1M')
        self.variables['fraud_loan_platform_30d_score'] = get_factor_field_value(df,
                                                                            'FIN_Loan_Plantform_all_0M_1M')
        self.variables['fraud_loan_small_30d_score'] = get_factor_field_value(df,
                                                                                 'FIN_Loan_Small_all_0M_1M')

        df_temp = df[df['field_name'].isin(['FIN_Bank_Type_Commercial_all_0M_1M', 'FIN_Bank_Type_Foreign_all_0M_1M',
                                            'FIN_Bank_Type_ShareCommercial_all_0M_1M', 'FIN_Bank_Type_Town_all_0M_1M',
                                            'FIN_Bank_Type_TownCredit_all_0M_1M', 'FIN_Bank_Type_Industrial_all_0M_1M'])]
        if not df_temp.empty:
            df_temp['field_value_1'] = df_temp.apply(lambda x:0 if pd.isna(x['field_value']) else float(x['field_value']))
            self.variables['fraud_bank_other_30d_score'] = df_temp['field_value_1'].sum()

    def clean_variables_cts(self):
        df = self._info_risk_cts_item()
        if df.empty:
            return
        df_temp = cts_match(df, ['cts_lbs_014', 'cts_lbs_021', 'cts_lbs_007'])
        self.variables['fraud_trace_cnt'] = 1 if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_001', 'cts_lbs_023', 'cts_lbs_028', 'cts_lbs_043'])
        self.variables['fraud_wifi_cnt'] = 1 if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_004', 'cts_lbs_010'])
        self.variables['fraud_act_cnt'] = 1 if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_msg_002', 'cts_msg_006', 'cts_msg_018', 'cts_msg_017'])
        self.variables['fraud_msg_cnt'] = df_temp.shape[0] if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_014'])
        self.variables['fraud_trace_city_180_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_021'])
        self.variables['fraud_trace_city_30_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_007'])
        self.variables['fraud_trace_city_15_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_001'])
        self.variables['fraud_trace_wifi_15_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_023'])
        self.variables['fraud_trace_wifi_60_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_028'])
        self.variables['fraud_trace_wifi_90_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_043'])
        self.variables['fraud_trace_wifi_9_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_msg_002'])
        self.variables['fraud_msg_fin_1_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_msg_006'])
        self.variables['fraud_msg_loan_3_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_msg_018'])
        self.variables['fraud_msg_fin_9_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_msg_017'])
        self.variables['fraud_msg_loan_9_cnt'] = int(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_032'])
        cts_app_032 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        df_temp = cts_match(df, ['cts_app_029'])
        cts_app_029 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        if cts_app_029 > 0:
            self.variables['fraud_loan_app_unistall_180_ratio'] = round(cts_app_032 / cts_app_029 * 100, 3)

        df_temp = cts_match(df, ['cts_app_094'])
        self.variables['fraud_loan_app_unistall_90_ratio'] = round(
            float(df_temp.loc[0, 'field_value']) * 100, 3) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_075'])
        self.variables['fraud_loan_app_unistall_60_ratio'] = round(
            float(df_temp.loc[0, 'field_value']) * 100, 3) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_058'])
        cts_app_058 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        df_temp = cts_match(df, ['cts_app_056'])
        cts_app_056 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        if cts_app_056 > 0:
            self.variables['fraud_loan_app_unistall_30_ratio'] = round(cts_app_058 / cts_app_056 * 100, 3)

        df_temp = cts_match(df, ['cts_app_006'])
        cts_app_006 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        df_temp = cts_match(df, ['cts_app_004'])
        cts_app_004 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        if cts_app_004 > 0:
            self.variables['fraud_loan_app_unistall_15_ratio'] = round(cts_app_006 / cts_app_004 * 100, 3)

        df_temp = cts_match(df, ['cts_app_041'])
        cts_app_041 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        df_temp = cts_match(df, ['cts_app_038'])
        cts_app_038 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        if cts_app_038 > 0:
            self.variables['fraud_fin_app_unistall_180_ratio'] = round(cts_app_041 / cts_app_038 * 100, 3)

        df_temp = cts_match(df, ['cts_app_099'])
        self.variables['fraud_fin_app_unistall_90_ratio'] = round(
            float(df_temp.loc[0, 'field_value']) * 100, 3) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_081'])
        self.variables['fraud_fin_app_unistall_60_ratio'] = round(
            float(df_temp.loc[0, 'field_value']) * 100, 3) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_064'])
        self.variables['fraud_fin_app_unistall_30_ratio'] = round(
            float(df_temp.loc[0, 'field_value']) * 100, 3) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_010'])
        self.variables['fraud_fin_app_unistall_15_ratio'] = round(
            float(df_temp.loc[0, 'field_value']) * 100, 3) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_004'])
        if not df_temp.empty:
            cts_lbs_004 = float(df_temp.loc[0, 'field_value'])
            if 0 < cts_lbs_004 < 1:
                cts_lbs_004 = 1
            elif cts_lbs_004 > 10:
                cts_lbs_004 = 10
            self.variables['fraud_trace_act_day_area'] = round(cts_lbs_004 / float(10), 1)

        df_temp = cts_match(df, ['cts_lbs_010'])
        if not df_temp.empty:
            cts_lbs_010 = float(df_temp.loc[0, 'field_value'])
            if 0 < cts_lbs_010 < 1:
                cts_lbs_010 = 1
            elif cts_lbs_010 > 10:
                cts_lbs_010 = 10
            self.variables['fraud_trace_act_night_area'] = round(cts_lbs_010 / float(10), 1)

    def transform(self):
        if 'company' in self.base_type:
            return
        self.clean_variables_cts()
        self.clean_variables_factor()
