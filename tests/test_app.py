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
            "reqNo": uuid.uuid4(),
            "stepReqNo": "1",
            "productCode": "abc",
            "bizType": ['01001', '02001', '05001', '05002', '06001', '07001', '08001', '09001', '10001', '11001', '12001', '13001', '14001', '16001', '17001', '18001', 'f0001', 'f0002', 'f0003'],
            "queryData": {
                "name": "刘劭卓",
                "idno": "430105199106096118",
                "phone": "11111111111",
                "userType":"PERSONAL"
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
