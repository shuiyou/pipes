#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json

import pytest
from jsonpath import jsonpath

from app import app
from logger.logger_util import LoggerUtil
from mapping import mysql_reader
from view.mapper_detail import round_max

logger = LoggerUtil().logger(__name__)


@pytest.fixture
def client():
    client = app.test_client()
    yield client


def test_shake_hand(client):
    rv = client.post('/biz-types', json={"reqNo": "Q348578907643084800", "productCode": "001",
                                         "queryData": {"name": "测试一", "idno": "421003198904091086",
                                                       "phone": "18516315591", "userType": "PERSONAL"},
                                         "versionNo": "1.0"})
    assert rv.status_code == 200
    v = rv.get_json()
    print(json.dumps(v))
    # assert v.get('bizTypes')[0] == '05002'


def test_strategy(client):
    rv = client.post('/strategy', json={
        "strategyParam": {"reqNo": "Q348791397404540928", "stepReqNo": "S348791397404540929", "productCode": "001",
                          "queryData": {"name": "测试一", "idno": "421003198904091086", "phone": "18516315591",
                                        "userType": "PERSONAL"},
                          "bizType": ["01001", "02001", "05001", "05002", "06001", "07001", "08001", "09001", "10001",
                                      "11001", "12001", "13001", "14001", "16001", "17001", "18001", "f0001", "f0002",
                                      "f0003"], "versionNo": "1.0"}, "strategyResult": {}})
    assert rv.status_code == 200
    v = rv.get_json()
    print(v)
    # assert v.get('bizTypes')[0] == '05002'


def test_sql_to_df():
    df = mysql_reader.sql_to_df("channel")
    print(df)


def test_json_path():
    json_obj = {
        "strategyParam": {
            "bizType": [
                "fffff"
            ],
            "productCode": "test10086",
            "queryData": {
                "idno": "421003198904091087",
                "name": "测试二",
                "phone": "18516315592",
                "userType": "PERSONAL"
            },
            "reqNo": "Q344619174854819841",
            "stepReqNo": "S344619174854819841",
            "versionNo": "1.0"
        },
        "strategyResult": {
            "StrategyOneResponse": {
                "Header": {
                    "InquiryCode": "Q344619174854819841",
                    "ProcessCode": "Level1_m",
                    "OrganizationCode": "",
                    "ProcessVersion": 10,
                    "LayoutVersion": 6
                },
                "Body": {
                    "Application": {
                        "Variables": {
                            "out_strategyBranch": "fffff",
                            "out_isQuery": "N",
                            "score_fraud": 30,
                            "score_debit": 0,
                            "score_credit": 0,
                            "score_business": 0,
                            "score_black": 100,
                            "score": 100,
                            "SCORE_GE_RAW": 27,
                            "out_result": "A",
                            "level": "高",
                            "level_black": "高",
                            "level_business": "低",
                            "level_credit": "低",
                            "level_debit": "低",
                            "level_fraud": "低",
                            "l_m_critical_score": 40,
                            "m_h_critical_score": 70
                        },
                        "Categories": [
                            {
                                "Reason": {
                                    "Variables": {
                                        "out_decisionBranchCode": "C001",
                                        "out_ReasonCode": "RR201"
                                    }
                                }
                            },
                            {
                                "Reason": {
                                    "Variables": {
                                        "out_decisionBranchCode": "UT001",
                                        "out_ReasonCode": "RY202",
                                        "queryData": [
                                            {
                                                "name": "关联公司1",
                                                "idno": "421003198904091087"
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "Reason": {
                                    "Variables": {
                                        "out_decisionBranchCode": "NT002",
                                        "out_ReasonCode": "RY502",
                                        "queryData": [
                                            {
                                                "name": "测试二",
                                                "idno": "421003198904091087"
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "Reason": {
                                    "Variables": {
                                        "out_decisionBranchCode": "NT003",
                                        "out_ReasonCode": "RY502",
                                        "queryData": [
                                            {
                                                "name": "测试二",
                                                "idno": "421003198904091087"
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "Reason": {
                                    "Variables": {
                                        "out_decisionBranchCode": "Z001",
                                        "out_ReasonCode": "RR209",
                                        "queryData": [
                                            {
                                                "name": "测试二",
                                                "idno": "421003198904091087"
                                            }
                                        ]
                                    }
                                }
                            },
                            {
                                "Reason": {
                                    "Variables": {
                                        "out_decisionBranchCode": "Z006",
                                        "out_ReasonCode": "RR601",
                                        "queryData": [
                                            {
                                                "name": "测试二",
                                                "idno": "421003198904091087"
                                            }
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        },
        "reportDetail":
            {

                "variables": {
                    "anti_tel_apply_bank_7d": 0,
                    "anti_id_apply_bank_7d": 0,
                    "anti_apply_bank_7d": 0,
                    "anti_tel_apply_bank_1m": 0,
                    "anti_id_apply_bank_1m": 0,
                    "anti_apply_bank_1m": 0,
                    "anti_tel_apply_bank_3m": 0,
                    "anti_id_apply_bank_3m": 0,
                    "anti_apply_bank_3m": 0,
                    "anti_tel_apply_sloan_7d": 0,
                    "anti_id_apply_sloan_7d": 0,
                    "anti_apply_sloan_7d": 0,
                    "anti_tel_apply_sloan_1m": 0,
                    "anti_id_apply_sloan_1m": 0,
                    "anti_apply_sloan_1m": 0,
                    "anti_tel_apply_sloan_3m": 0,
                    "anti_id_apply_sloan_3m": 0,
                    "anti_apply_sloan_3m": 0,
                    "anti_tel_apply_p2p_7d": 0,
                    "anti_id_apply_p2p_7d": 0,
                    "anti_apply_p2p_7d": 0,
                    "anti_tel_apply_p2p_1m": 0,
                    "anti_id_apply_p2p_1m": 0,
                    "anti_apply_p2p_1m": 0,
                    "anti_tel_apply_p2p_3m": 0,
                    "anti_id_apply_p2p_3m": 0,
                    "anti_apply_p2p_3m": 0,
                    "anti_tel_apply_confin_7d": 0,
                    "anti_id_apply_confin_7d": 0,
                    "anti_apply_confin_7d": 0,
                    "anti_tel_apply_confin_1m": 0,
                    "anti_id_apply_confin_1m": 0,
                    "anti_apply_confin_1m": 0,
                    "anti_tel_apply_confin_3m": 0,
                    "anti_id_apply_confin_3m": 0,
                    "anti_apply_confin_3m": 0,
                    "anti_tel_apply_other_7d": 0,
                    "anti_id_apply_other_7d": 0,
                    "anti_apply_other_7d": 0,
                    "anti_tel_apply_other_1m": 0,
                    "anti_id_apply_other_1m": 0,
                    "anti_apply_other_1m": 0,
                    "anti_tel_apply_other_3m": 0,
                    "anti_id_apply_other_3m": 0,
                    "anti_apply_other_3m": 0,
                    "ovdu_overdue_time_amt": '',
                    "net_apply_bank_7d": 0,
                    "net_apply_bank_1m": 0,
                    "net_apply_bank_3m": 0,
                    "net_apply_sloan_7d": 0,
                    "net_apply_sloan_1m": 0,
                    "net_apply_sloan_3m": 0,
                    "net_apply_p2p_7d": 0,
                    "net_apply_p2p_1m": 0,
                    "net_apply_p2p_3m": 0,
                    "net_apply_confin_7d": 0,
                    "net_apply_confin_1m": 0,
                    "net_apply_confin_3m": 0,
                    "net_apply_other_7d": 0,
                    "net_apply_other_1m": 0,
                    "net_apply_other_3m": 0,
                    "net_apply_bank_6m": 0,
                    "net_apply_bank_12m": 0,
                    "net_apply_bank_his": 0,
                    "net_apply_sloan_6m": 0,
                    "net_apply_sloan_12m": 0,
                    "net_apply_sloan_his": 0,
                    "net_apply_p2p_6m": 0,
                    "net_apply_p2p_12m": 0,
                    "net_apply_p2p_his": 0,
                    "net_apply_confin_6m": 0,
                    "net_apply_confin_12m": 0,
                    "net_apply_confin_his": 0,
                    "net_apply_other_6m": 0,
                    "net_apply_other_12m": 0,
                    "net_apply_other_his": 0,
                    "base_date": "2019-07-12",
                    "base_idno": "",
                    "base_gender": 0,
                    "base_age": 0,
                    "base_black": 0,
                    "base_type": "PERSON"
                }
            }

    }
    branch_code = jsonpath(json_obj, '$.strategyResult.StrategyOneResponse.Body.Application.Categories..Variables')
    for c in branch_code:
        print(c['out_decisionBranchCode'])
        c['queryData'] = [{'name': 'abchello', 'idno': '421003198904091087'}]
    # print(json.dumps(json_obj))


def test_round_max():
    max_arr = [1, 2, 3]
    median_arr = [1, 2, 3]
    v = round_max(max_arr, median_arr, 0.3)
    assert v == 4.0
