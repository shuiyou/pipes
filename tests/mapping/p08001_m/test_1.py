import pandas as pd

from mapping.module_processor import ModuleProcessor


def _pawn_cnt():
    flow_df = pd.DataFrame( [['典当一次','典当2次','111'],
                             [1,2,3]
                            ] , columns = ['unusual_trans_type','id']
    )
    pawn = flow_df['unusual_trans_type'].str.contains('典当')
    pawn_cnt = flow_df['unusual_trans_type'][pawn].count()
    return pawn_cnt


paw = _pawn_cnt()
print(paw)