from mapping.t32001 import T32001


def test_001():
    ps = T32001()
    ps.run(user_name="ζθθΏ",id_card_no="430321198110270613",phone="")
    print(ps.variables)