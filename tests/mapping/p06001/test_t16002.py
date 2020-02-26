from mapping.p06001.t16002 import T16002
from mapping.p06001.t24001 import T24001
from mapping.p06001.tf0004 import Tf0004
from view.p06001.v16002 import V16002
from view.p06001.vf0004 import Vf0004
from view.p06001.v24001 import V24001
from mapping.utils.df_comparator_util import df_compare
import pandas as pd
import numpy as np
import json


def test_df_compare():
    old_df = pd.DataFrame(np.random.randint(30, 100, 5000), columns=['id'])
    new_df = old_df.copy()

    variables = {'test': 0}
    df_compare(variables, old_df, new_df, 'test')
    print(variables)


def test_set_compare():
    old_df = pd.DataFrame(np.random.randint(30, 100, 5000), columns=['id'])
    new_df = old_df.copy()

    diff = set(list(new_df['id'])).difference(list(old_df['id']))
    print(len(diff))


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
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    print(ps.variables)


def test_v16002():
    ps = V16002()
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    with open('e.txt', 'w') as f:
        f.write(json.dumps(ps.variables))
    # print(ps.variables)


def test_vf0004():
    ps = Vf0004()
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    with open('f.txt', 'w') as f:
        f.write(json.dumps(ps.variables))
    # print(ps.variables)


def test_v24001():
    ps = V24001()
    ps.run(user_name='上海点牛信息科技集团有限公司', id_card_no='91310230MA1K05K83G',
           origin_data={'preBizDate': '2019-10-01'})
    with open('f.txt', 'w') as f:
        f.write(json.dumps(ps.variables))
    # print(ps.variables)
