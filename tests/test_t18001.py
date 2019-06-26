from mapping.t18001 import T18001


def test_phone_on_line_days():
    t = T18001()
    t.run(user_name='段娜', id_card_no='411403198609101683')
    print(t.variables)

