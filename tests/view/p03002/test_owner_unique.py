from view.p03002.owner_unique import Owner


def test_001():
    ps = Owner()
    ps.run(user_name="江西鼎信日用品有限公司",id_card_no="91360121MA38JRU16A",origin_data={"extraParam":{"strategy":"01"},"user_name":"江西鼎信日用品有限公司","id_card_no":"91360121MA38JRU16A","baseType":"U_PER_SP_SH_M_COMPANY"})
    print(ps.variables)