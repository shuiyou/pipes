from mapping.t12001 import T12001

def test_t12001():
    ps1 = T12001()
    ps1.transform('徐隆','310226199209140310','13916199275')
    assert ps1.variables == {'anti_idno_consume_black': 0, 'anti_idno_P2P_black': 0, 'anti_idno_lost_black': 0, 'anti_idno_fraud_black': 0, 'anti_idno_loan_black': 0, 'anti_tel_industy_black': 0, 'anti_tel_P2P_black': 0, 'anti_tel_lost_black': 0, 'anti_tel_fraud_black': 0, 'anti_tel_small_no': 0, 'anti_idno_court_execu': 0, 'anti_idno_court_closure': 0, 'anti_idno_court_break_faith': 0, 'anti_idno_legal_break_faith': 0, 'anti_idno_apply_1m': 0, 'anti_idno_apply_3m': 0, 'anti_tel_apply_1m': 0, 'anti_tel_apply_3m': 0, 'anti_idno_apply_3d': 0, 'anti_idno_apply_7d': 0, 'anti_tel_apply_3d': 0, 'anti_tel_apply_7d': 0}

    ps2 = T12001()
    ps2.transform('阮荷根','332603196807186393','13970926568')
    assert ps2.variables == {'anti_idno_consume_black': 0, 'anti_idno_P2P_black': 0, 'anti_idno_lost_black': 0, 'anti_idno_fraud_black': 0, 'anti_idno_loan_black': 0, 'anti_tel_industy_black': 0, 'anti_tel_P2P_black': 0, 'anti_tel_lost_black': 0, 'anti_tel_fraud_black': 0, 'anti_tel_small_no': 0, 'anti_idno_court_execu': 0, 'anti_idno_court_closure': 0, 'anti_idno_court_break_faith': 0, 'anti_idno_legal_break_faith': 0, 'anti_idno_apply_1m': 0, 'anti_idno_apply_3m': 0, 'anti_tel_apply_1m': 0, 'anti_tel_apply_3m': 0, 'anti_idno_apply_3d': 0, 'anti_idno_apply_7d': 0, 'anti_tel_apply_3d': 0, 'anti_tel_apply_7d': 0}

    ps3 = T12001()
    ps3.transform('莫四', '450404198601010031','13722345670')
    assert ps3.variables == {'anti_idno_consume_black': 0, 'anti_idno_P2P_black': 0, 'anti_idno_lost_black': 0, 'anti_idno_fraud_black': 0, 'anti_idno_loan_black': 0, 'anti_tel_industy_black': 0, 'anti_tel_P2P_black': 0, 'anti_tel_lost_black': 0, 'anti_tel_fraud_black': 0, 'anti_tel_small_no': 0, 'anti_idno_court_execu': 0, 'anti_idno_court_closure': 0, 'anti_idno_court_break_faith': 0, 'anti_idno_legal_break_faith': 0, 'anti_idno_apply_1m': 0, 'anti_idno_apply_3m': 0, 'anti_tel_apply_1m': 0, 'anti_tel_apply_3m': 0, 'anti_idno_apply_3d': 0, 'anti_idno_apply_7d': 0, 'anti_tel_apply_3d': 0, 'anti_tel_apply_7d': 0}


