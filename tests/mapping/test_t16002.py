from mapping.t16002 import T16002


def test_ps_court_info():
    ps = T16002()
    ps.run(user_name='爱俊', id_card_no='', phone='')
    print(ps.variables)
    # assert ps.variables['court_ent_admi_vio'] == 2
    # assert ps.variables['court_ent_admi_vio_amt_3y'] == 384.82
    # assert ps.variables['court_ent_judge'] == 2
    # assert ps.variables['court_ent_docu_status'] == 3
    # assert ps.variables['court_ent_judge_amt_3y'] == 687690.21
    # assert ps.variables['court_ent_trial_proc'] == 2
    # assert ps.variables['court_ent_proc_status'] == 2
    # assert ps.variables['court_ent_tax_pay'] == 1
    # assert ps.variables['court_ent_owed_owe'] == 1
    # assert ps.variables['court_ent_dishonesty'] == 1
    # assert ps.variables['court_ent_limit_entry'] == 0
    # assert ps.variables['court_ent_high_cons'] == 1
    # assert ps.variables['court_ent_pub_info'] == 2
    # assert ps.variables['court_ent_pub_info_amt_3y'] == 2217000.00
    # assert ps.variables['court_ent_cri_sus'] == 0
