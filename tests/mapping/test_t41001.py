# @Time : 2020/4/23 7:27 PM 
# @Author : lixiaobo
# @File : test_t41001.py.py 
# @Software: PyCharm
from mapping.t41001 import T41001


def test_t41001_mapping():
    t41001 = T41001()
    origin_data = {
        "preReportReqNo": "47832748273423435",
        "extraParam": {

        }
    }
    t41001.run(user_name='施网明', id_card_no='310108196610024859', phone='13277154945', origin_data=origin_data, cached_data= {})
    print(t41001.variables)

