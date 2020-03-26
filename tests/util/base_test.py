def test_base1():
    variables_mapping = {
        "法院失信名单": "mag_court_break_faith",
        "限制高消费名单": "mag_court_high_cons",
        "借款合同纠纷": "mag_court_loan_con",
        "民间借贷纠纷": "mag_court_pop_loan",
        "第三方标注黑名单": "mag_fraudinfo_isMachdBlMakt",
        "身份证和姓名在黑名单": "mag_social_idc_name_in_black",
        "姓名和手机号在黑名单": "mag_social_name_tel_in_black"
    }

    for k in variables_mapping:
        print(type(k), k, variables_mapping.get(k))
