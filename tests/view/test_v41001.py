import json

from view.v41001 import V41001


def test_v41001():
    ps = V41001()
    ps.run(user_name='施网明', id_card_no='310108196610024859', phone='13277154945', origin_data={"preBizDate": "2019-11-04 10:24:33"})
    print(json.dumps(ps.variables))
