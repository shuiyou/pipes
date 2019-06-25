from mapping.t16002 import T16002

def test_ps_court_administrative_violation():
    ps = T16002()
    df = ps._court_administrative_violation_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_administrative_violation(df)
    print(ps.variables)
    assert ps.variables['court_ent_admi_vio'] == 2
    assert ps.variables['court_ent_admi_vio_amt_3y'] == 434.82

def test_ps_court_judicative_pape():
    ps = T16002()
    df = ps._court_judicative_pape_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_judicative_pape(df)
    print(ps.variables)
    assert ps.variables['court_ent_judge'] == 2
    assert ps.variables['court_ent_docu_status'] == 1
    assert ps.variables['court_ent_judge_amt_3y'] == 687690.21

def test_ps_court_trial_process():
    ps = T16002()
    df = ps._court_trial_process_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_trial_process(df)
    print(ps.variables)
    assert ps.variables['court_ent_trial_proc'] == 2
    assert ps.variables['court_ent_proc_status'] == 2

def test_ps_court_taxable_abnormal_user():
    ps = T16002()
    df = ps._court_taxable_abnormal_user_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_taxable_abnormal_user(df)
    assert ps.variables['court_ent_tax_pay'] == 1

def test__ps_court_arrearage():
    ps = T16002()
    df = ps._court_arrearage_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_arrearage(df)
    assert ps.variables['court_ent_owed_owe'] == 1

def test_ps_court_deadbeat():
    ps = T16002()
    df = ps._court_deadbeat_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_deadbeat(df)
    assert ps.variables['court_ent_dishonesty'] == 1

def test_ps_court_limited_entry_exit():
    ps = T16002()
    df = ps._court_limited_entry_exit_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_limited_entry_exit(df)
    assert ps.variables['court_ent_limit_entry'] == 0

def test_ps_court_limit_hignspending():
    ps = T16002()
    df = ps._court_limit_hignspending_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_limit_hignspending(df)
    assert ps.variables['court_ent_high_cons'] == 1

def test_ps_court_excute_public():
    ps = T16002()
    df = ps._court_excute_public_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_excute_public(df)
    print(ps.variables)
    assert ps.variables['court_ent_pub_info'] == 2
    assert ps.variables['court_ent_pub_info_amt_3y'] == 22517.00

def test_ps_court_criminal_suspect():
    ps = T16002()
    df = ps._court_criminal_suspect_df("吉林华正农牧业开发股份有限公司")
    ps._ps_court_criminal_suspect(df)
    assert ps.variables['court_ent_cri_sus'] == 0
