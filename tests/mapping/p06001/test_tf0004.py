from mapping.p06001.tf0004 import Tf0004


def test_tf0004():
    ps = Tf0004()
    ps.run(user_name='施网明', id_card_no='310108196610024859',
           origin_data={'preBizDate': '2019-10-01'})
    print(ps.variables)
