from view.p03002.basic_unique import BasicUnique


def test_001():
    ps = BasicUnique()
    ps.run(user_name="上海语诺工程装饰材料有限公司",id_card_no="91310114774792910B",origin_data={"extraParam":{"strategy":"01"}})
    print(ps.variables)