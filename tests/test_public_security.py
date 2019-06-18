from mapping.public_security import ps_name_id


def test_ps_name_id():
    result = ps_name_id(user_name='任震东', id_card_no='150304197609302532')
    print("result is" + str(result))
    assert result == 0
