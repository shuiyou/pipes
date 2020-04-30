# @Time : 2020/4/30 5:24 PM 
# @Author : lixiaobo
# @File : calculator.py.py 
# @Software: PyCharm
import pandas as pd


def sum_duration_max(df, val_lists):
    val = []
    pre_val = None
    for item_df in df.itertuples():
        curr_val = item_df.status
        curr_amt = item_df.repayment_amt
        if pd.notna(curr_val) and curr_val.isdigit():
            if len(val) == 0:
                val.append(curr_amt)
            elif pre_val:
                if pre_val + 1 == int(curr_val):
                    val.append(curr_amt)
                elif len(val) > 0:
                    val_lists.append(val)
                    val = [curr_amt]
            pre_val = int(curr_val)
        else:
            if len(val) > 0:
                val_lists.append(val)
            val = []
            pre_val = None

    if len(val) > 0:
        val_lists.append(val)
        val = []
