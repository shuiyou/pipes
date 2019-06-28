from mapping.tf0003 import Tf0003

def test_case_info():
    ps = Tf0003()
    ps.run(user_name='张小江', id_card_no='130637198205101819', phone='')
    print(ps.variables)






