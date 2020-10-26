from mapping.t61001 import T61001


def test_001():
    ps = T61001()
    ps.run(user_name="易胜进",id_card_no="430321198110270613",phone="")
    print(ps.variables)