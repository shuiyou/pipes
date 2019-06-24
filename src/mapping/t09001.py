from mapping.tranformer import Transformer
from mapping.mysql_reader import sql_to_df

class T09001(Transformer):
    """
    逾期核查
    """

    def __init__(self,user_name,id_card_no) -> None:
        super().__init__()
        self.user_name = user_name
        self.id_card_no = id_card_no

        self.variables = {
            'qh_loanee_apro_cnt_6m': 0,
            'qh_loanee_hit_org_cnt': 0,
            'qh_loanee_hit_bank_cnt': 0,
            'qh_loanee_hit_finance_cnt': 0,
            'qh_loanee_hit_p2p_cnt': 0,
            'qh_loanee_hit_org_cnt_3m': 0,
            'qh_loanee_query_mac_cnt_6m': 0,
        }

    def _loan_other_df(self,id_no):
        info_loan_other = """
            SELECT A.reg_date as reg_date,A.loanee_json_id as loanee_json_id,B.reason_code,B.industry,B.amount,B.bnk_amount,
            B.cnss_amount,B.p2p_amount,B.query_amt,B.query_amt_m3,B.query_amt_m6,B.busi_date
            FROM (SELECT batch_no,reg_date,loanee_json_id FROM qh_loanee_json WHERE id_no = %(id_no)s  ORDER BY reg_date desc LIMIT 1) as A ,qh_loanee as B
            WHERE A.batch_no = B.batch_no
        """
        df = sql_to_df(sql=info_loan_other,
                       params={"id_no": id_no})
        return df

    def _ps_loan_other(self, df=None):
        if df is not None and len(df) > 0:
            self.variables['qh_loanee_apro_cnt_6m'] = ""
            self.variables['qh_loanee_hit_org_cnt'] = df['数量'][0]
            self.variables['qh_loanee_hit_bank_cnt'] = df['银行'][0]
            self.variables['qh_loanee_hit_finance_cnt'] = df['消费金融'][0]
            self.variables['qh_loanee_hit_p2p_cnt'] = df['p2p'][0]
            self.variables['qh_loanee_hit_p2p_cnt'] = df['p2p'][0]
            self.variables['qh_loanee_hit_org_cnt_3m'] = ""
            self.variables['qh_loanee_query_mac_cnt_6m'] = df['6个月'][0]

    def transform(self, user_name=None, id_card_no=None, phone=None):
        pass
