import pandas as pd

from mapping.t07001 import T07001

df = pd.DataFrame({'channel_score': [3], 'fail_rate': [2], 'detail_data': [
    '{"resCode":"0000","resMsg":"提交成功","data":{"statusCode":"2012","statusMsg":"查询成功","result":{"evaluate":{"score":"486","failRate":"0.33"},"detail":[{"cardId":"131942239","transLastDate":"20171116","transFirstDate":"20171014","latest_1M_OrgCount":"1","latest_1M_FailOrgCount":"1","latest_1M_TransCount":"2","latest_1M_FailTransCount":"1","lowBalance_1M_FailCount":"1","success_1M_PayTotalAmount":"1323.83","success_1M_PayMaxAmount":"1323.83","success_1M_PayAvgAmount":"1323.83","latest_1M_OtherPayOrgCount":"","success_1M_OtherPayTotalAmount":"","success_1M_OtherPayMaxAmount":"","success_1M_OtherPayAvgAmount":"","latest_3M_OrgCount":"1","latest_3M_FailOrgCount":"1","latest_3M_TransCount":"3","latest_3M_FailTransCount":"1","lowBalance_3M_FailCount":"1","success_3M_PayTotalAmount":"2647.66","success_3M_PayMaxAmount":"1323.83","success_3M_PayAvgAmount":"1323.83","latest_3M_OtherPayOrgCount":"","success_3M_OtherPayTotalAmount":"","success_3M_OtherPayMaxAmount":"","success_3M_OtherPayAvgAmount":"","latest_6M_OrgCount":"1","latest_6M_FailOrgCount":"1","latest_6M_TransCount":"3","latest_6M_FailTransCount":"1","lowBalance_6M_FailCount":"1","success_6M_PayTotalAmount":"2647.66","success_6M_PayMaxAmount":"1323.83","success_6M_PayAvgAmount":"1323.83","latest_6M_OtherPayOrgCount":"","success_6M_OtherPayTotalAmount":"","success_6M_OtherPayMaxAmount":"","success_6M_OtherPayAvgAmount":"","latest_12M_OrgCount":"1","latest_12M_FailOrgCount":"1","latest_12M_TransCount":"3","latest_12M_FailTransCount":"1","lowBalance_12M_FailCount":"1","success_12M_PayTotalAmount":"2647.66","success_12M_PayMaxAmount":"1323.83","success_12M_PayAvgAmount":"1323.83","latest_12M_OtherPayOrgCount":"","success_12M_OtherPayTotalAmount":"","success_12M_OtherPayMaxAmount":"","success_12M_OtherPayAvgAmount":"","fundOpen6mHave":"","fundOpen6mCount":"","fundPur1m":"","fundPurEqy1m":"","fundPurMoy1m":"","fundPurBond1m":"","transCountPur1m":"","successCountPur1m":"","amountSuccessPur1m":"","amountMaxPur1m":"","amountAvgPur1m":"","fundApi1m":"","fundApiEqy1m":"","fundApiMoy1m":"","fundApiBond1m":"","transCountApi1m":"","successCountApi1m":"","amountSuccessApi1m":"","amountMaxApi1m":"","amountAvgApi1m":"","fundDiv1m":"","fundDivEqy1m":"","fundDivMoy1m":"","fundDivBond1m":"","transCountDiv1m":"","successCountDiv1m":"","amountSuccessDiv1m":"","amountMaxDiv1m":"","amountAvgDiv1m":"","fundRedm1m":"","fundRedmEqy1m":"","fundRedmMoy1m":"","fundRedmBond1m":"","transCountRedm1m":"","successCountRedm1m":"","amountSuccessRedm1m":"","amountMaxRedm1m":"","amountAvgRedm1m":"","fundPur12m":"","fundPurEqy12m":"","fundPurMoy12m":"","fundPurBond12m":"","transCountPur12m":"","successCountPur12m":"","amountSuccessPur12m":"","amountMaxPur12m":"","amountAvgPur12m":"","fundApi12m":"","fundApiEqy12m":"","fundApiMoy12m":"","fundApiBond12m":"","transCountApi12m":"","successCountApi12m":"","amountSuccessApi12m":"","amountMaxApi12m":"","amountAvgApi12m":"","fundDiv12m":"","fundDivEqy12m":"","fundDivMoy12m":"","fundDivBond12m":"","transCountDiv12m":"","successCountDiv12m":"","amountSuccessDiv12m":"","amountMaxDiv12m":"","amountAvgDiv12m":"","fundRedm12m":"","fundRedmEqy12m":"","fundRedmMoy12m":"","fundRedmBond12m":"","transCountRedm12m":"","successCountRedm12m":"","amountSuccessRedm12m":"","amountMaxRedm12m":"","amountAvgRedm12m":""}]}}}']})


def test_lend_score():
    t = T07001()
    t._lend_score(df)
    print(t.variables)


def test_lend_fail_rate():
    t = T07001()
    t._lend_fail_rate(df)
    print(t.variables)


def test_lend_cha_cnt_12m():
    t = T07001()
    t._lend_cha_cnt_12m(df)
    print(t.variables)


def test_lend_chafail_cnt_12m():
    t = T07001()
    t._lend_chafail_cnt_12m(df)
    print(t.variables)
