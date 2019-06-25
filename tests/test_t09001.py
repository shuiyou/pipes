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
    ps._ps_loan_date(df)
    print(df)
    assert ps.variables['qh_loanee_apro_cnt_6m'] == 2
    assert ps.variables['qh_loanee_hit_org_cnt_3m'] == 1

def test_get_oney_from_string():
    value = "一、撤销湖北省武汉市中级人民法院(2015)鄂武汉中行终字第00569号行政判决；二、维持武汉市黄陂区人民法院(2015)鄂黄陂行初字第00030号行政判决。一、二审案件受理费各50元，均由武汉市黄陂区农业机械有限责任公司负担。本判决为终审判决。"
    value = re.sub(r"(第?[0-9]\d*号)", "", value)
    value = re.sub(r"\([0-9]{4}\)", "", value)
    value = re.sub(r"〔[0-9]{4}〕", "", value)
    value = re.sub(r"(http://)((\w)|(\W)|(\.))*", "", value)
    moneyArray = re.findall(r"\d+\.?\d*",value)
    moneyMax = 0
    for money in moneyArray:
        if ('万元') in value:
            moneyRe = float(money)*10000
        else:
            moneyRe = float(money)
        if moneyRe > moneyMax:
            moneyMax = moneyRe
    print("%.2f" %moneyMax)

def test_match_string():
    #value = "一、撤销湖北省武汉市中级人民法院(2015)鄂武汉中行终字第00569号行政判决；二、维持武汉市黄陂区人民法院(2015)鄂黄陂行初字第00030号行政判决。一、二审案件受理费各50元，均由武汉市黄陂区农业机械有限责任公司负担。本判决为终审判决。"
    #value = "浦市监案处字〔2016〕第150201610141号"
    value = "浦市监案处字〔2016〕第150201610141号http://hb.bjchy.gov.cn/xxgk/zwgk/xzcfxzxk/xzcf/201709/P020170901624301324525.zip"
    value = re.sub(r"(第?[0-9]\d*号)","",value)
    value = re.sub(r"\([0-9]{4}\)","",value)
    value = re.sub(r"〔[0-9]{4}〕","",value)
    value = re.sub(r"(http://)((\w)|(\W)|(\.))*","",value)
    print(value)


def test_number_transfer():
    value = 5.112
    print(int(value))