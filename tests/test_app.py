#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json

import pytest

from app import app
from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


@pytest.fixture
def client():
    client = app.test_client()
    yield client


def resource_content(file_name):
    f = open('resource/' + file_name, 'r', encoding='UTF-8')
    str_content = f.read()
    f.close()
    return str_content


def shake_hand_request(client, product_code):
    content = resource_content("shake_hand_p" + product_code + ".txt")
    rv = client.post('/biz-types', json=json.loads(content))
    assert rv.status_code == 200
    v = rv.get_json()
    print(json.dumps(v))


def strategy_request(client, product_code):
    content = resource_content("strategy_p" + product_code + ".txt")
    rv = client.post('/strategy', json=json.loads(content))
    assert rv.status_code == 200
    v = rv.get_json()
    print(json.dumps(v))


# 一级个人握手
def test_shake_hand_001(client):
    shake_hand_request(client, "001")


# 一级个人决策
def test_strategy_001(client):
    strategy_request(client, "001")


# 一级企业握手shake_hand_request
def test_shake_hand_002(client):
    shake_hand_request(client, "002")


# 一级企业决策
def test_strategy_002(client):
    strategy_request(client, "002")


# 一级联合握手
def test_shake_hand_p003(client):
    shake_hand_request(client, "003")


# 一级联合决策
def test_strategy_p003(client):
    strategy_request(client, "003")


# 灰名单移除握手
def test_shake_hand_005(client):
    shake_hand_request(client, "005")


# 灰名单移除决策
def test_strategy_005(client):
    strategy_request(client, "005")





