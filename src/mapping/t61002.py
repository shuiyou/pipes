from mapping.tranformer import Transformer


class T61002(Transformer):
    """
       白骑士特殊识别
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