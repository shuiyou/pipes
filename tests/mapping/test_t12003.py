from mapping.t12003 import T12003


def test_001():
    ps = T12003()
    ps.run(user_name="ζθθΏ",id_card_no="430321198110270613",phone="")
    print(ps.variables)