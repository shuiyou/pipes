#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import uuid

import pytest

from app import app
from logger.logger_util import LoggerUtil
from mapping import mysql_reader

logger = LoggerUtil().logger(__name__)


@pytest.fixture
def client():
    client = app.test_client()
    yield client


def test_shake_hand(client):
    rv = client.post('/biz-types', json={
        "reqNo": uuid.uuid4(),
        "productCode": "1",
        "queryData":{
            "name":"刘劭卓",
            "idno":"430105199106096118",
            "phone":"11111111111",
            "userType":"PERSONAL"
        }
    })
    assert rv.status_code == 200
    v = rv.get_json()
    print(v)
    # assert v.get('bizTypes')[0] == '05002'


def test_strategy(client):
    rv = client.post('/strategy', json={
        "strategyParam": {
            "reqNo": "请求编号",
            "stepReqNo": "子请求编号",
            "productCode": "产品编号",
            "bizType": ['1111', '3333'],
            "queryData": {
                "name": "名称",
                "idno": "证件号码",
                "phone": "手机号"
            },
        },
        "strategyResult": {}
    })
    assert rv.status_code == 200
    v = rv.get_json()
    print(v)
    # assert v.get('bizTypes')[0] == '05002'


def test_sql_to_df():
    df = mysql_reader.sql_to_df("channel")
    print(df)
