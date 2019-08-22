from mapping.t13001 import T13001


def test_t13001():
    # ps1 = T13001()
    # ps1.run('冯乃根', '460025198308294818', '12345678910')
    # assert ps1.variables == {'sms_reg_cnt': 27, 'sms_reg_cnt_bank_3m': 0, 'sms_reg_cnt_other_3m': 0, 'sms_app_cnt': 5,
    #                          'sms_max_apply': 4, 'sms_loan_cnt': 5, 'sms_max_loan': 4, 'sms_reject_cnt': 0,
    #                          'sms_overdue_cnt': 0, 'sms_max_overdue': 0, 'sms_owe_cnt': 0, 'sms_max_owe': 0,
    #                          'sms_owe_cnt_6m': 0, 'sms_owe_cnt_6_12m': 0, 'sms_max_owe_6m': 0}
    #
    # ps2 = T13001()
    # ps2.run('刘金双', '420983197106025610', '13166256990')
    # assert ps2.variables == {'sms_reg_cnt': 1, 'sms_reg_cnt_bank_3m': 0, 'sms_reg_cnt_other_3m': 0, 'sms_app_cnt': 1,
    #                          'sms_max_apply': 3, 'sms_loan_cnt': 1, 'sms_max_loan': 3, 'sms_reject_cnt': 0,
    #                          'sms_overdue_cnt': 0, 'sms_max_overdue': 0, 'sms_owe_cnt': 0, 'sms_max_owe': 0,
    #                          'sms_owe_cnt_6m': 0, 'sms_owe_cnt_6_12m': 0, 'sms_max_owe_6m': 0}
    ps2 = T13001()
    ps2.run('拓秀华', '23120119850605723X', '18170219039')
    print(ps2.variables)