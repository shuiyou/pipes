import pandas as pd

from mapping.tranformer import Transformer

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class V12001(Transformer):
    """
    反欺诈相关的变量模块
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'anti_tel_apply_bank_7d': 0,
            'anti_id_apply_bank_7d': 0,
            'anti_apply_bank_7d': 0,
            'anti_tel_apply_bank_1m': 0,
            'anti_id_apply_bank_1m': 0,
            'anti_apply_bank_1m': 0,
            'anti_tel_apply_bank_3m': 0,
            'anti_id_apply_bank_3m': 0,
            'anti_apply_bank_3m': 0,
            'anti_tel_apply_sloan_7d': 0,
            'anti_id_apply_sloan_7d': 0,
            'anti_apply_sloan_7d': 0,
            'anti_tel_apply_sloan_1m': 0,
            'anti_id_apply_sloan_1m': 0,
            'anti_apply_sloan_1m': 0,
            'anti_tel_apply_sloan_3m': 0,
            'anti_id_apply_sloan_3m': 0,
            'anti_apply_sloan_3m': 0,
            'anti_tel_apply_p2p_7d': 0,
            'anti_id_apply_p2p_7d': 0,
            'anti_apply_p2p_7d': 0,
            'anti_tel_apply_p2p_1m': 0,
            'anti_id_apply_p2p_1m': 0,
            'anti_apply_p2p_1m': 0,
            'anti_tel_apply_p2p_3m': 0,
            'anti_id_apply_p2p_3m': 0,
            'anti_apply_p2p_3m': 0,
            'anti_tel_apply_confin_7d': 0,
            'anti_id_apply_confin_7d': 0,
            'anti_apply_confin_7d': 0,
            'anti_tel_apply_confin_1m': 0,
            'anti_id_apply_confin_1m': 0,
            'anti_apply_confin_1m': 0,
            'anti_tel_apply_confin_3m': 0,
            'anti_id_apply_confin_3m': 0,
            'anti_apply_confin_3m': 0,
            'anti_tel_apply_other_7d': 0,
            'anti_id_apply_other_7d': 0,
            'anti_apply_other_7d': 0,
            'anti_tel_apply_other_1m': 0,
            'anti_id_apply_other_1m': 0,
            'anti_apply_other_1m': 0,
            'anti_tel_apply_other_3m': 0,
            'anti_id_apply_other_3m': 0,
            'anti_apply_other_3m': 0
        }


#  执行变量转换
def transform(self):
    pass
