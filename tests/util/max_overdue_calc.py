# @Time : 2020/4/30 5:15 PM 
# @Author : lixiaobo
# @File : max_overdue_calc.py.py 
# @Software: PyCharm
import pandas as pd

from mapping.p07001_m.calculator import sum_duration_max


def test_df9():
    df = pd.DataFrame([
        ["*", 1],
        ["1", 2],
        ["2", 3],
        ["3", 4],
        [None, 5],
        [None, 6],
        ["1", 7],
        ["*", 8],
        ["N", 9],
        ["N", 10],
        ["1", 11],
        ["2", 1],
        ["N", 2],
        ["1", 3],
        ["*", 4],
        ["N", 5],
        ["N", 6],
        ["1", 7],
        ["N", 8],
        ["1", 9],
        ["2", 1],
        ["3", 2],
        ["4", 3],
        ["N", 4],
    ], columns=["status", "repayment_amt"])

    val_lists = []
    sum_duration_max(df, val_lists)

    for item in val_lists:
        print(item, "\n")

    final_result = sum(map(lambda x: max(x), val_lists))
    print("final_result:", final_result)