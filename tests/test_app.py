#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
    rv = client.get('/shake-hand', json={
        "productCode": "JB_WZ_CJR2"
    })
    assert rv.status_code == 200
    logger.info(rv.get_json())


def test_sql_to_df():
    df = mysql_reader.sql_to_df("channel")
    print(df)
