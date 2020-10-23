from file_utils.files import file_content

from view.p03002.basic_union import BasicUnion


def test_001():
    ps = BasicUnion()
    ps.run(user_name="上海语诺工程装饰材料有限公司",id_card_no="91310114774792910B",origin_data={"extraParam":{"strategy":"01"}})
    print(ps.variables)