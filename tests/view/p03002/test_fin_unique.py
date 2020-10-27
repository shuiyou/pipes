from view.p03002.fin_unique import Fin


def test_001():
    ps = Fin()
    ps.run(user_name="徐少春", id_card_no="321281198701227939",
           origin_data={"extraParam": {"strategy": "01"}, "name": "徐少春", "idno": "321281198701227939",
                        'baseType': "U_PERSONAL"})
    print(ps.variables)

