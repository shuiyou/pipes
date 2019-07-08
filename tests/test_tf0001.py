from mapping.tf0001 import Tf0001


def test_tf_0001():
    ps = Tf0001()
    ps.run(user_name='', id_card_no='352230198512260015', phone='')
    print(ps.variables)
    assert ps.variables['relent_court_open_admi_violation'] == 3
    assert ps.variables['relent_court_admi_violation_max'] == 128461.54
    assert ps.variables['relent_court_open_judge_docu'] == 3
    assert ps.variables['relent_court_judge_max'] == 23344.90
    assert ps.variables['relent_court_open_judge_proc'] == 2
    assert ps.variables['relent_court_open_tax_pay'] == 2
    assert ps.variables['relent_court_open_owed_owe'] == 2
    assert ps.variables['relent_court_open_tax_arrears'] == 2
    assert ps.variables['relent_court_open_court_dishonesty'] == 2
    assert ps.variables['relent_court_open_rest_entry'] == 2
    assert ps.variables['relent_court_open_high_cons'] == 2
    assert ps.variables['relent_court_open_cri_sus'] == 2
    assert ps.variables['relent_court_open_fin_loan_con'] == 1
    assert ps.variables['relent_court_open_loan_con'] == 1
    assert ps.variables['relent_court_open_pop_loan'] == 1
    assert ps.variables['relent_court_open_docu_status'] == 2
    assert ps.variables['relent_court_open_proc_status'] == 2
    assert ps.variables['relent_court_tax_arrears_max'] == 1233.77
    assert ps.variables['relent_court_pub_info_max'] == 22200.00
