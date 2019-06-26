from mapping.t17001 import T17001



def test_t17001():
    ps1 = T17001()
    ps1.run(user_name='仲小梅', id_card_no='321284198501083626', phone='15052304168')
    assert ps1.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 0,
                             'net_tel_risk_l': 1, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
                             'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 1, 'net_idno_veh': 0,
                             'net_idno_risk_m': 0, 'net_idno_risk_l': 1, 'net_idno_debt': 0,
                             'net_idno_tax': 0, 'net_idno_tax_rep': 0, 'net_idno_repay': 0,
                             'net_risk_tel_hit_ovdu': 0,'net_risk_tel_hit_high_att': 1, 'net_risk_idc_hit_ovdu': 0,
                             'net_risk_idc_hit_court_dish': 0,'net_risk_idc_hit_high_att': 0, 'net_bah_1d_dev_rel_tel': 0,
                             'net_bah_1d_dev_rel_idc': 0,'net_bah_1d_idc_rel_dev': 0, 'net_bah_1d_tel_rel_dev': 0,
                             'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0, 'net_bah_7d_tel_app': 0,
                             'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0, 'net_bah_7d_idc_rel_dev': 0,
                             'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0, 'net_bah_1m_idc_app': 0,
                             'net_bah_1m_tel_app': 0, 'net_bah_1m_idc_rel_dev': 0, 'net_bah_3m_add_rel_idc': 0,
                             'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0, 'net_bah_3m_idc_rel_tel': 0,
                             'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0, 'net_bah_3m_tel_rel_bctel': 0,
                             'net_bah_3m_mail_rel_idc': 0,'net_bah_3m_tel_rel_idc': 0, 'net_apply_7d': 2,
                             'net_apply_1m': 3, 'net_apply_3m': 5, 'net_apply_6m': 5, 'net_apply_12m': 6,
                             'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0, 'net_idc_name_hit_exec_vague': 0,
                             'net_applicant_idc_3m_morethan2': 0, 'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 97}

    ps2 = T17001()
    ps2.run(user_name='丁炎滨', id_card_no='370983199111010012', phone='13761955305')
    assert ps2.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 0,
                             'net_tel_risk_l': 0, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
                             'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 0, 'net_idno_veh': 0,
                             'net_idno_risk_m': 0, 'net_idno_risk_l': 0, 'net_idno_debt': 0, 'net_idno_tax': 0,
                             'net_idno_tax_rep': 0, 'net_idno_repay': 0, 'net_risk_tel_hit_ovdu': 0,
                             'net_risk_tel_hit_high_att': 0, 'net_risk_idc_hit_ovdu': 0,
                             'net_risk_idc_hit_court_dish': 0, 'net_risk_idc_hit_high_att': 0,
                             'net_bah_1d_dev_rel_tel': 0, 'net_bah_1d_dev_rel_idc': 0, 'net_bah_1d_idc_rel_dev': 0,
                             'net_bah_1d_tel_rel_dev': 0, 'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0,
                             'net_bah_7d_tel_app': 0, 'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0,
                             'net_bah_7d_idc_rel_dev': 0, 'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0,
                             'net_bah_1m_idc_app': 0, 'net_bah_1m_tel_app': 0, 'net_bah_1m_idc_rel_dev': 0,
                             'net_bah_3m_add_rel_idc': 0, 'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0,
                             'net_bah_3m_idc_rel_tel': 0, 'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0,
                             'net_bah_3m_tel_rel_bctel': 0, 'net_bah_3m_mail_rel_idc': 0, 'net_bah_3m_tel_rel_idc': 0,
                             'net_apply_7d': 0, 'net_apply_1m': 0, 'net_apply_3m': 0, 'net_apply_6m': 0,
                             'net_apply_12m': 0, 'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0,
                             'net_idc_name_hit_exec_vague': 0, 'net_applicant_idc_3m_morethan2': 0,
                             'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 0}

    ps3 = T17001()
    ps3.run(user_name='万国梁', id_card_no='360123198508011930', phone='18516326459')
    assert ps3.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 0,
                             'net_tel_risk_l': 0, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
                             'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 0, 'net_idno_veh': 0,
                             'net_idno_risk_m': 0, 'net_idno_risk_l': 0, 'net_idno_debt': 0, 'net_idno_tax': 0,
                             'net_idno_tax_rep': 0, 'net_idno_repay': 0, 'net_risk_tel_hit_ovdu': 0,
                             'net_risk_tel_hit_high_att': 0, 'net_risk_idc_hit_ovdu': 0,
                             'net_risk_idc_hit_court_dish': 0, 'net_risk_idc_hit_high_att': 0,
                             'net_bah_1d_dev_rel_tel': 0, 'net_bah_1d_dev_rel_idc': 0, 'net_bah_1d_idc_rel_dev': 0,
                             'net_bah_1d_tel_rel_dev': 0, 'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0,
                             'net_bah_7d_tel_app': 0, 'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0,
                             'net_bah_7d_idc_rel_dev': 0, 'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0,
                             'net_bah_1m_idc_app': 0, 'net_bah_1m_tel_app': 0, 'net_bah_1m_idc_rel_dev': 0,
                             'net_bah_3m_add_rel_idc': 0, 'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0,
                             'net_bah_3m_idc_rel_tel': 0, 'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0,
                             'net_bah_3m_tel_rel_bctel': 0, 'net_bah_3m_mail_rel_idc': 0, 'net_bah_3m_tel_rel_idc': 0,
                             'net_apply_7d': 0, 'net_apply_1m': 0, 'net_apply_3m': 0, 'net_apply_6m': 0,
                             'net_apply_12m': 0, 'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0,
                             'net_idc_name_hit_exec_vague': 0, 'net_applicant_idc_3m_morethan2': 0,
                             'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 0}

    ps4 = T17001()
    ps4.run(user_name='云高琼', id_card_no='422826198009300047', phone='15347020567')
    assert ps4.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 1,
                             'net_tel_risk_l': 0, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
                             'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 0, 'net_idno_veh': 0,
                             'net_idno_risk_m': 1, 'net_idno_risk_l': 0, 'net_idno_debt': 0, 'net_idno_tax': 0,
                             'net_idno_tax_rep': 0, 'net_idno_repay': 0, 'net_risk_tel_hit_ovdu': 0,
                             'net_risk_tel_hit_high_att': 0, 'net_risk_idc_hit_ovdu': 0,
                             'net_risk_idc_hit_court_dish': 0, 'net_risk_idc_hit_high_att': 0,
                             'net_bah_1d_dev_rel_tel': 0, 'net_bah_1d_dev_rel_idc': 0, 'net_bah_1d_idc_rel_dev': 0,
                             'net_bah_1d_tel_rel_dev': 0, 'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0,
                             'net_bah_7d_tel_app': 0, 'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0,
                             'net_bah_7d_idc_rel_dev': 0, 'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0,
                             'net_bah_1m_idc_app': 0, 'net_bah_1m_tel_app': 0, 'net_bah_1m_idc_rel_dev': 0,
                             'net_bah_3m_add_rel_idc': 0, 'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0,
                             'net_bah_3m_idc_rel_tel': 0, 'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0,
                             'net_bah_3m_tel_rel_bctel': 0, 'net_bah_3m_mail_rel_idc': 0, 'net_bah_3m_tel_rel_idc': 0,
                             'net_apply_7d': 0, 'net_apply_1m': 3, 'net_apply_3m': 6, 'net_apply_6m': 6,
                             'net_apply_12m': 11, 'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0,
                             'net_idc_name_hit_exec_vague': 0, 'net_applicant_idc_3m_morethan2': 0,
                             'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 28}

    ps5 = T17001()
    ps5.run(user_name='何淼君', id_card_no='332621197205058252', phone='13117174542')
    assert ps5.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 0,
                             'net_tel_risk_l': 0, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
                             'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 0, 'net_idno_veh': 0,
                             'net_idno_risk_m': 0, 'net_idno_risk_l': 0, 'net_idno_debt': 0, 'net_idno_tax': 0,
                             'net_idno_tax_rep': 0, 'net_idno_repay': 0, 'net_risk_tel_hit_ovdu': 0,
                             'net_risk_tel_hit_high_att': 0, 'net_risk_idc_hit_ovdu': 0,
                             'net_risk_idc_hit_court_dish': 0, 'net_risk_idc_hit_high_att': 0,
                             'net_bah_1d_dev_rel_tel': 0, 'net_bah_1d_dev_rel_idc': 0, 'net_bah_1d_idc_rel_dev': 0,
                             'net_bah_1d_tel_rel_dev': 0, 'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0,
                             'net_bah_7d_tel_app': 0, 'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0,
                             'net_bah_7d_idc_rel_dev': 0, 'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0,
                             'net_bah_1m_idc_app': 0, 'net_bah_1m_tel_app': 0, 'net_bah_1m_idc_rel_dev': 0,
                             'net_bah_3m_add_rel_idc': 0, 'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0,
                             'net_bah_3m_idc_rel_tel': 0, 'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0,
                             'net_bah_3m_tel_rel_bctel': 0, 'net_bah_3m_mail_rel_idc': 0, 'net_bah_3m_tel_rel_idc': 0,
                             'net_apply_7d': 0, 'net_apply_1m': 0, 'net_apply_3m': 0, 'net_apply_6m': 0,
                             'net_apply_12m': 0, 'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0,
                             'net_idc_name_hit_exec_vague': 0, 'net_applicant_idc_3m_morethan2': 0,
                             'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 0}

    ps6 = T17001()
    ps6.run(user_name='余瑞泉', id_card_no='350623198102251016', phone='13817853005')
    assert ps6.variables == {'net_tel_small': 0, 'net_tel_virtual': 0, 'net_tel_fraud': 0, 'net_tel_risk_m': 0,
                             'net_tel_risk_l': 0, 'net_tel_veh': 0, 'net_tel_debt': 0, 'net_tel_repay': 0,
                             'net_idno_crime': 0, 'net_idno_exec': 0, 'net_idno_end': 0, 'net_idno_veh': 0,
                             'net_idno_risk_m': 0, 'net_idno_risk_l': 0, 'net_idno_debt': 0, 'net_idno_tax': 0,
                             'net_idno_tax_rep': 0, 'net_idno_repay': 0, 'net_risk_tel_hit_ovdu': 0,
                             'net_risk_tel_hit_high_att': 0, 'net_risk_idc_hit_ovdu': 0,
                             'net_risk_idc_hit_court_dish': 0, 'net_risk_idc_hit_high_att': 0,
                             'net_bah_1d_dev_rel_tel': 0, 'net_bah_1d_dev_rel_idc': 0, 'net_bah_1d_idc_rel_dev': 0,
                             'net_bah_1d_tel_rel_dev': 0, 'net_bah_7d_dev_app': 0, 'net_bah_7d_idc_app': 0,
                             'net_bah_7d_tel_app': 0, 'net_bah_7d_dev_rel_idc': 0, 'net_bah_7d_dev_rel_tel': 0,
                             'net_bah_7d_idc_rel_dev': 0, 'net_bah_7d_tel_rel_dev': 0, 'net_bah_1m_dev_app': 0,
                             'net_bah_1m_idc_app': 0, 'net_bah_1m_tel_app': 0, 'net_bah_1m_idc_rel_dev': 0,
                             'net_bah_3m_add_rel_idc': 0, 'net_bah_3m_bcname_rel_idc': 0, 'net_bah_3m_idc_rel_add': 0,
                             'net_bah_3m_idc_rel_tel': 0, 'net_bah_3m_idc_rel_bctel': 0, 'net_bah_3m_idc_rel_mail': 0,
                             'net_bah_3m_tel_rel_bctel': 0, 'net_bah_3m_mail_rel_idc': 0, 'net_bah_3m_tel_rel_idc': 0,
                             'net_apply_7d': 0, 'net_apply_1m': 0, 'net_apply_3m': 0, 'net_apply_6m': 0,
                             'net_apply_12m': 0, 'net_risk_age_high': 0, 'net_idc_name_hit_dish_vague': 0,
                             'net_idc_name_hit_exec_vague': 0, 'net_applicant_idc_3m_morethan2': 0,
                             'net_applicant_tel_3m_morethan2': 0, 'net_final_score': 0}
