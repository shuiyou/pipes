from mapping.p06001.t16002 import T16002
from mapping.p06001.t24001 import T24001
from mapping.p06001.tf0004 import Tf0004
from view.p06001.v16002 import V16002
from view.p06001.vf0004 import Vf0004
from view.p06001.v24001 import V24001


def test_t16002():
    ps = T16002()
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    print(ps.variables)


def test_t24001():
    ps = T24001()
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    print(ps.variables)


def test_tf0004():
    ps = Tf0004()
    ps.run(user_name='施网明', id_card_no='310108196610024859',
           origin_data={'preBizDate': '2019-10-01'})
    print(ps.variables)


def test_v16002():
    ps = V16002()
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    print(ps.variables)


def test_vf0004():
    ps = Vf0004()
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    print(ps.variables)


def test_v24001():
    ps = V24001()
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    print(ps.variables)
