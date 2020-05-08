# -*- coding: utf-8 -*-
import pandas as pd

from mapping.t05001 import T05001
from mapping.t05002 import T05002
from mapping.t06001 import T06001
import datetime
import json
import numpy as np


def test_ps_name_id():
    ps = T05002()
    mock_df = pd.DataFrame({
        'result': [b'\x01']
    })
    ps._ps_name_id(df=mock_df)
    assert ps.variables['ps_name_id'] == 0


def test_phone_check():
    ps = T05001()
    ps.run(user_name='伊春光', id_card_no='412827199102225781', phone='15317063103')
    print(ps.variables)
    ps.run(user_name='仝太宝', id_card_no='342225198604171519', phone='13671770773')
    print(ps.variables)


def test_ps_crime_type():
    ps = T06001()

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': [''],
        'case_period': ['[0,3)']
    }))
    print(ps.variables)

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['DRUG_RELATED, ILLEGAL_F']
    }))
    assert ps.variables['ps_involve_drug'] == 1
    assert ps.variables['ps_drug'] == 0

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['DRUG_RELATED, DRUG']
    }))
    assert ps.variables['ps_involve_drug'] == 1
    assert ps.variables['ps_drug'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[0, 3)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[3, 6)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[6, 12)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 1

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[12, 24)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 2

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[24, 50)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 3

    ps._ps_crime_type(pd.DataFrame({
        'crime_type': ['ILLEGAL_A'],
        'case_period': ['[0,0)']
    }))
    assert ps.variables['ps_illeg_crim'] == 1
    assert ps.variables['ps_illegal_record_time'] == 4


def test_df():
    data={'a':[3,3],'c':[2,3],'e':[1,2]}
    df=pd.DataFrame(data)
    # data1={"b":[1,3,5,6,7,3,3,10],"d":['aaa','bbb','ccc','ddd','eee','fff','gggg','kkk']}
    # df1=pd.DataFrame(data1)
    # df2=pd.merge(df,df1,left_on='a',right_on='b')
    # df['d']=df.apply(get_credit_min_repay,axis=1,args=('a','c'))
    print(df['a'].drop_duplicates().size)



def test_datetime():
    # now =datetime.datetime.now()
    # print(now-datetime.timedelta(days=3))
    # list=[1,np.nan,2,3]
    # print(json.dumps(list))
    a,b=3,2
    print(['否','是'][a>b])


def get_credit_min_repay(df,repay_amount,amount_replay_amount):
    return ['否','是'][df[repay_amount]*2>df[amount_replay_amount]]


