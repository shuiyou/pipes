# @Time : 2020/4/23 7:27 PM 
# @Author : lixiaobo
# @File : test_t41001.py.py 
# @Software: PyCharm
from mapping.t41001 import T41001


def test_t41001_mapping():
    t41001 = T41001()
    t41001.run(user_name='上海点牛互联网金融信息服务有限公司', id_card_no='91310000MA1K32DCXU', phone='11111111111')
    print(t41001.variables)

