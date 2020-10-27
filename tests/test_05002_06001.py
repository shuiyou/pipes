# -*- coding: utf-8 -*-
import re

from jsonpath import jsonpath
import pandas as pd
from file_utils.files import file_content

from mapping.t05001 import T05001
from mapping.t05002 import T05002
from mapping.t06001 import T06001
import math
import time
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
    data={'a':['2020-09','2020-05','2020-06','2020-07'],'c':['10','2','3','4'],'e':[2,2,3,4]}
    data1={'a':['2020-09','2020-05','2020-06','2020-07'],'c':['10','2','3','4'],'e':[2,2,3,4]}
    df=pd.DataFrame(data)
    df1=pd.DataFrame(data1)
    # data1={"d":[1,3],"h":['aaa','bbb']}
    # df1=pd.DataFrame(data1)
    # df2=pd.merge(df,df1,left_on='a',right_on='d')
    # df['d']=df.apply(get_credit_min_repay,axis=1,args=('a','c'))
    diff_set=set(df1.iloc[:, 0]).difference(set(df.iloc[:, 0]))
    value = 0 if len(diff_set) == 0 else 1
    print(value)


def test_datetime():
    data={'借据编号':['1','2'],'借据起期':[20190521,20190521]}
    df=pd.DataFrame(data=data)
    df=df.astype(object)
    df['借据起期']=pd.to_datetime(df['借据起期'])
    # df['借据起期']=df['借据起期'].apply(lambda x:datetime.datetime.strptime(str(x), '%Y%m%d'))
    print(df)



def get_credit_min_repay(df,repay_amount,amount_replay_amount):
    return ['否','是'][df[repay_amount]*2>df[amount_replay_amount]]


def test_get_data():
    msg = file_content("./resource", "unin_level1_001.json")
    data = json.loads(msg)
    query_data_list = jsonpath(data, '$..queryData[*]')
    resp = []
    for query_data in query_data_list:
        name = query_data.get("name")
        idno = query_data.get("idno")
        user_type = query_data.get("userType")
        print(user_type)
        strategy = query_data.get("strategy")
        print(strategy)
        if user_type == 'COMPANY' and strategy == '01':
            resp_dict = {"name": name, "idno": idno}
            resp.append(resp_dict)
    print(resp)

def test_002():
    data = {'借据编号': ['申请执行人dddd', '2'], '借据起期': [20190521, 20190521]}
    df = pd.DataFrame(data=data)
    print(df[df['借据编号']])

def test_003():
    data = "(0.24,0.333]"
    print(type(re.findall(r"\((.+?)\," ,data)[0]))
    print(float('-999') == -999)





