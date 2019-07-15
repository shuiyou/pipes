from mapping.tranformer import Transformer



class V09001(Transformer):
    """
    他贷核查
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'oth_loan_apply_bank_7d': 0,
            'oth_loan_apply_bank_1m': 0,
            'oth_loan_apply_bank_3m': 0,
            'oth_loan_apply_sloan_7d': 0,
            'oth_loan_apply_sloan_1m': 0,
            'oth_loan_apply_sloan_3m': 0,
            'oth_loan_apply_p2p_7d': 0,
            'oth_loan_apply_p2p_1m': 0,
            'oth_loan_apply_p2p_3m': 0,
            'oth_loan_apply_confin_7d': 0,
            'oth_loan_apply_confin_1m': 0,
            'oth_loan_apply_confin_3m': 0,
            'oth_loan_apply_other_7d': 0,
            'oth_loan_apply_other_1m': 0,
            'oth_loan_apply_other_3m': 0,
            'oth_loan_apply_bank_6m': 0,
            'oth_loan_apply_bank_12m': 0,
            'oth_loan_apply_bank_his': 0,
            'oth_loan_apply_sloan_6m': 0,
            'oth_loan_apply_sloan_12m': 0,
            'oth_loan_apply_sloan_his': 0,
            'oth_loan_apply_p2p_6m': 0,
            'oth_loan_apply_p2p_12m': 0,
            'oth_loan_apply_p2p_his': 0,
            'oth_loan_apply_confin_6m': 0,
            'oth_loan_apply_confin_12m': 0,
            'oth_loan_apply_confin_his': 0,
            'oth_loan_apply_other_6m': 0,
            'oth_loan_apply_other_12m': 0,
            'oth_loan_apply_other_his': 0
        }


    def transform(self):
        pass