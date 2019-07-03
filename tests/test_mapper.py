from mapping.mapper import translate, get_transformer
import pandas as pd


def test_type():
    t05002 = get_transformer('05002')
    t05002.run(user_name='张虎', id_card_no='430122197512087812')
    print(t05002.variables)
    assert t05002.variables['ps_name_id'] == 0


def test_transform():
    codes=['16001']
    res = translate(codes, user_name='昌鹏', id_card_no='540228195907066426')
    # df = pd.DataFrame(res,pd.Index(range(1)))
    # print(df['aaa'])
    print(res)
    for key in res:
       if 'base_date'==key:
           print(res[key])
