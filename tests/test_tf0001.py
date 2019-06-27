from mapping.tf0001 import TF0001

def test_tf_0001():
    ps = TF0001()
    ps.run(user_name='', id_card_no='352230198512260015', phone='')
    print(ps.variables)