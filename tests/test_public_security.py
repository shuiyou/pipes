from mapping.public_security import PublicSecurity


def test_ps_name_id():
    ps = PublicSecurity(user_name='任震东', id_card_no='150304197609302532')
    result = ps.ps_name_id()
    print("result is" + str(result))
    assert result == 0
