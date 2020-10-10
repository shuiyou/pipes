from mapping.tranformer import Transformer


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
            'i_cnt_partner_all_Imbank_180day': 0,
            'm_max_cnt_partner_daily_all_Unconsumerfinance_365day': 0,
            'm_ratio_cnt_grp_id_Loan_all_all': 0,
            'm_length_first_last_all_Imbank_60day':0,
            'i_length_first_last_all_Imbank_365day':0
        }



    def transform(self):
        pass