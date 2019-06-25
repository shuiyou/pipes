from mapping.tranformer import Transformer, subtract_datetime_col
from mapping.mysql_reader import sql_to_df


class T09001(Transformer):
    """
    逾期核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'qh_loanee_apro_cnt_6m': 0,
            'qh_loanee_hit_org_cnt': 0,
            'qh_loanee_hit_bank_cnt': 0,
            'qh_loanee_hit_finance_cnt': 0,
            'qh_loanee_hit_p2p_cnt': 0,
            'qh_loanee_hit_org_cnt_3m': 0,
            'qh_loanee_query_mac_cnt_6m': 0
        }

    def _loan_other_df(self,id_card_no):
        info_loan_other = """
            SELECT reason_code,industry,amount,bank_amount,
            consumer_finance_amount,p_2_p_amount,query_amount,query_amount_three_month,query_amount_six_month,data_build_time,create_time
            FROM info_risk_other_loan_record WHERE other_loan_id = (SELECT other_loan_id FROM info_risk_other_loan WHERE id_card_no = %(id_card_no)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY expired_at desc LIMIT 1) LIMIT 1
        """
        df = sql_to_df(sql=info_loan_other,
                       params={"id_card_no": id_card_no})
        return df

    def _ps_loan_other(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['qh_loanee_hit_org_cnt'] = df['amount'][0]
            self.variables['qh_loanee_hit_bank_cnt'] = df['bank_amount'][0]
            self.variables['qh_loanee_hit_finance_cnt'] = df['consumer_finance_amount'][0]
            self.variables['qh_loanee_hit_p2p_cnt'] = df['p_2_p_amount'][0]
            self.variables['qh_loanee_query_mac_cnt_6m'] = df['query_amount_six_month'][0]

    def _loan_other_date_df(self,id_card_no):
        info_loan_other = """
            SELECT (TO_DAYS(create_time) - TO_DAYS(data_build_time)) as days,reason_code FROM info_risk_other_loan_record WHERE other_loan_id = (
	        SELECT other_loan_id FROM info_risk_other_loan WHERE id_card_no = %(id_card_no)s and 
            unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY expired_at desc LIMIT 1)
        """
        df = sql_to_df(sql=info_loan_other,
                       params={"id_card_no": id_card_no})
        return df

    def _loan_date_df(self,id_card_no):
        info_loan_other = """
        SELECT B.data_build_time as data_build_time,B.reason_code as reason_code,A.create_time as create_time FROM info_risk_other_loan_record B ,(
	    SELECT other_loan_id,create_time FROM info_risk_other_loan WHERE id_card_no = %(id_card_no)s and 
        unix_timestamp(NOW()) < unix_timestamp(expired_at)  ORDER BY expired_at desc LIMIT 1) A
        WHERE B.other_loan_id = A.other_loan_id            
        """
        df = sql_to_df(sql=info_loan_other,
                       params={"id_card_no": id_card_no})
        self.diff_month = subtract_datetime_col(df, 'create_time', 'data_build_time', 'M')
        return df


    def _ps_loan_date(self,df=None):
        if df is not None and len(df) > 0:
            #self.variables['qh_loanee_apro_cnt_6m'] = df['create_time'].shape[0]
            self.variables['qh_loanee_apro_cnt_6m'] = df.query(self.diff_month + ' < 6').shape[0]
            self.variables['qh_loanee_hit_org_cnt_3m'] = df.query(self.diff_month + '< 3').shape[0]

    def _ps_loan_other_date(self,df=None):
        if df is not None and len(df) > 0:
            self.variables['qh_loanee_apro_cnt_6m'] = df.query('reason_code == "01" and days < 180').shape[0]
            self.variables['qh_loanee_hit_org_cnt_3m'] = df.query('days < 90').shape[0]

    def transform(self, user_name=None, id_card_no=None, phone=None):
        self._ps_loan_other(self._loan_other_df(self.id_card_no))
        self._ps_loan_other_date(self._loan_other_date_df(self.id_card_no))

