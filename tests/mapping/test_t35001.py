from mapping.t35001 import T35001


def test_001():
    ps = T35001()
    ps.run(user_name="ζθθΏ",id_card_no="430321198110270613",phone="13960612450")
    print(ps.variables)