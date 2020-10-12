from mapping.tranformer import Transformer
from util.mysql_reader import sql_to_df


def cts_match(df, params):
    return df[df['field_name'].isin(params)]


class Fraud(Transformer):

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
            'fraud_bank_app_avg_score': 0,
            'fraud_loan_app_avg_score': 0,
            'fraud_pay_app_avg_score': 0,
            'fraud_fintool_app_avg_score': 0,
            'fraud_invest_app_avg_score': 0,
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
            'fraud_loan_app_unistall_180_avg_ratio': 0,
            'fraud_loan_app_unistall_90_avg_ratio': 0,
            'fraud_loan_app_unistall_60_avg_ratio': 0,
            'fraud_loan_app_unistall_30_avg_ratio': 0,
            'fraud_loan_app_unistall_15_avg_ratio': 0,
            'fraud_fin_app_unistall_180_avg_ratio': 0,
            'fraud_fin_app_unistall_90_avg_ratio': 0,
            'fraud_fin_app_unistall_60_avg_ratio': 0,
            'fraud_fin_app_unistall_30_avg_ratio': 0,
            'fraud_fin_app_unistall_15_avg_ratio': 0,
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

    def clean_variables_cts(self, df):
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
        self.variables['fraud_trace_city_180_cnt'] = int(df_temp.loc[0,'field_value']) if not df_temp.empty else 0

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
            self.variables['fraud_loan_app_unistall_180_ratio'] = "%.1f%%" % (cts_app_032 / cts_app_029 * 100)

        df_temp = cts_match(df, ['cts_app_094'])
        self.variables['fraud_loan_app_unistall_90_ratio'] = "%.1f%%" % (
                float(df_temp.loc[0, 'field_value']) * 100) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_075'])
        self.variables['fraud_loan_app_unistall_60_ratio'] = "%.1f%%" % (
                    float(df_temp.loc[0, 'field_value']) * 100) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_058'])
        cts_app_058 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        df_temp = cts_match(df, ['cts_app_056'])
        cts_app_056 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        if cts_app_056 > 0:
            self.variables['fraud_loan_app_unistall_30_ratio'] = "%.1f%%" % (cts_app_058 / cts_app_056 * 100)

        df_temp = cts_match(df, ['cts_app_006'])
        cts_app_006 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        df_temp = cts_match(df, ['cts_app_004'])
        cts_app_004 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        if cts_app_004 > 0:
            self.variables['fraud_loan_app_unistall_15_ratio'] = "%.1f%%" % (cts_app_006 / cts_app_004 * 100)

        df_temp = cts_match(df, ['cts_app_041'])
        cts_app_041 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        df_temp = cts_match(df, ['cts_app_038'])
        cts_app_038 = float(df_temp.loc[0, 'field_value']) if not df_temp.empty else 0
        if cts_app_038 > 0:
            self.variables['fraud_fin_app_unistall_180_ratio'] = "%.1f%%" % (cts_app_041 / cts_app_038 * 100)

        df_temp = cts_match(df, ['cts_app_099'])
        self.variables['fraud_fin_app_unistall_90_ratio'] = "%.1f%%" % (
                float(df_temp.loc[0, 'field_value']) * 100) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_081'])
        self.variables['fraud_fin_app_unistall_60_ratio'] = "%.1f%%" % (
                float(df_temp.loc[0, 'field_value']) * 100) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_064'])
        self.variables['fraud_fin_app_unistall_30_ratio'] = "%.1f%%" % (
                float(df_temp.loc[0, 'field_value']) * 100) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_app_010'])
        self.variables['fraud_fin_app_unistall_15_ratio'] = "%.1f%%" % (
                float(df_temp.loc[0, 'field_value']) * 100) if not df_temp.empty else 0

        df_temp = cts_match(df, ['cts_lbs_004'])
        if not df_temp.empty:
            cts_lbs_004 = float(df_temp.loc[0, 'field_value'])
            if 0 < cts_lbs_004 < 1:
                cts_lbs_004 = 1
            elif cts_lbs_004 > 10:
                cts_lbs_004 = 10
            self.variables['fraud_trace_act_day_area'] = round(cts_lbs_004/float(10),1)

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
        df = self._info_risk_cts_item()
        self.clean_variables_cts(df)