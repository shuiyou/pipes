import pandas as pd
from mapping.t09001 import T09001
import re

def test_ps_loan_other():
    ps = T09001()
    df = ps._loan_other_df("410305197903240547")
    ps._ps_loan_other(df)
    assert ps.variables['qh_loanee_hit_org_cnt'] == 1
    assert ps.variables['qh_loanee_hit_bank_cnt'] == 2
    assert ps.variables['qh_loanee_hit_finance_cnt'] == 5
    assert ps.variables['qh_loanee_hit_p2p_cnt'] == 3
    assert ps.variables['qh_loanee_query_mac_cnt_6m'] == 6


def test_ps_loan_other_date():
    ps = T09001();
    df = ps._loan_other_date_df("410305197903240547")
    ps._ps_loan_other_date(df)
    assert ps.variables['qh_loanee_apro_cnt_6m'] == 2
    assert ps.variables['qh_loanee_hit_org_cnt_3m'] == 1

def test_ps_loan_date():
    ps = T09001();
    df = ps._loan_date_df("410305197903240547")
    # print(df['data_build_time'][0])
    # print(df['create_time'][0])
    # print(df.dtypes)
    print(df)
    ps._ps_loan_date(df)
    print(df)

def test_get_oney_from_string():
    value = "金额:23417.11,执行标的:154736.8"
    moneyArray = re.findall(r"\d+\.?\d*",value)
    print(moneyArray)
    moneyMax = float("0")
    for money in moneyArray:
        moneyRe = float(money)
        if moneyRe > moneyMax:
            moneyMax = moneyRe
    print(moneyMax)