#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json

import pandas as pd
from file_utils.files import resource_content

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)


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


# 一级联合决策
def test_strategy_p003_1(client):
    strategy_request(client, "003_1")


# 风险拦截握手
def test_shake_hand_p004(client):
    shake_hand_request(client, "004")


# 风险拦截决策
def test_strategy_p004(client):
    strategy_request(client, "004")


# 灰名单移除握手
def test_shake_hand_005(client):
    shake_hand_request(client, "005")


# 灰名单移除决策
def test_strategy_005(client):
    strategy_request(client, "005")


# 贷后报告握手
def test_shake_hand_06001(client):
    shake_hand_request(client, "06001")


# 贷后报告决策调用
def test_strategy_06001(client):
    strategy_request(client, "06001")


# 贷后报告握手
def test_shake_hand_06001_1(client):
    shake_hand_request(client, "06001_1")


# 征信报告请求
def test_strategy_07001(client):
    strategy_request(client, "07001")


# 征信报告请求
def test_strategy_07001_1(client):
    strategy_request(client, "07001_1")


# 征信报告请求
def test_strategy_07001_2(client):
    strategy_request(client, "07001_2")


# 征信拦截请求
def test_strategy_07002(client):
    strategy_request(client, "07002")
