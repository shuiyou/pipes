#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json

import pytest
from jsonpath import jsonpath

from app import app
from logger.logger_util import LoggerUtil
from util import mysql_reader
from view.mapper_detail import round_max
import requests
from config import STRATEGY_URL

logger = LoggerUtil().logger(__name__)


@pytest.fixture
def client():
    client = app.test_client()
    yield client


def test_shake_hand(client):
    rv = client.post('/biz-types', json={"reqNo": "Q351697278932779008", "productCode": "001",
                                         "queryData": {"name": "温烈祥", "idno": "362137198208311018",
                                                       "phone": "13761659574", "userType": "PERSONAL"},
                                         "versionNo": "1.0"})
    assert rv.status_code == 200
    v = rv.get_json()
    print(json.dumps(v))
    # assert v.get('bizTypes')[0] == '05002'


def test_shake_hand_p003(client):
    f = open('resource/shake_hand_p003.txt', 'r', encoding='UTF-8')
    str = f.read()
    f.close()
    rv = client.post('/biz-types-test', json=json.loads(str))
    v = rv.get_json()
    print(json.dumps(v))




def test_stratey_p003(client):
    f = open('resource/strategy_p003.txt', 'r', encoding='UTF-8')
    str = f.read()
    f.close()
    rv = client.post('/strategy-test', json=json.loads(str))
    v = rv.get_json()
    print(json.dumps(v))



def test_strategy(client):
    rv = client.post('/strategy', json={
        "strategyParam": {"reqNo": "Q356548494120615936", "stepReqNo": "S356548494120615937", "productCode": "001", "queryData": {"name": "施网明", "idno": "310108196610024859", "phone": "13301778997", "userType": "PERSONAL"}, "bizType": ["01001", "02001", "05001", "05002", "06001", "07001", "08001", "09001", "10001", "11001", "12001", "13001", "14001", "16001", "17001", "18001", "f0001", "f0002", "f0003"], "versionNo": "1.0"}})
    assert rv.status_code == 200
    v = rv.get_json()
    print(json.dumps(v))
    # assert v.get('bizTypes')[0] == '05002'

def test_strategy_p003(client):
    rv = client.post('/strategy', json={
        "strategyParam": {"reqNo": "Q356548494120615936", "stepReqNo": "S356548494120615937", "productCode": "001",
                          "queryData": {"name": "施网明", "idno": "310108196610024859", "phone": "13301778997",
                                        "userType": "PERSONAL"},
                          "bizType": ["01001", "02001", "05001", "05002", "06001", "07001", "08001", "09001", "10001",
                                      "11001", "12001", "13001", "14001", "16001", "17001", "18001", "f0001", "f0002",
                                      "f0003"], "versionNo": "1.0"}})
    assert rv.status_code == 200
    v = rv.get_json()
    print(json.dumps(v))


def test_qiye_strategy(client):
    rv = client.post('/strategy', json={
        "strategyParam": {
            "reqNo": "Q344619174854866677",
            "stepReqNo": "S344619174854819846",
            "productCode": "002",
            "queryData": {
                "name": "上海卫园餐饮管理有限公司",
                "idno": "91310115088687037K",
                "phone": "",
                "userType": "COMPANY"
            },
            "bizType": [
                "16002",
                "24001",
                "f0004",
                "f0005"
            ],
            "versionNo": "1.0"
        }
    })
    assert rv.status_code == 200
    v = rv.get_json()
    print(json.dumps(v))


def test_sql_to_df():
    df = mysql_reader.sql_to_df("channel")
    print(df)


def test_to_strategy():
    # 调用决策引擎
    strategy_response = requests.post(STRATEGY_URL, json={'StrategyOneRequest': {'Header': {'InquiryCode': 'Q360763137919713280', 'ProcessCode': 'Level1_m'}, 'Body': {'Application': {'Variables': {'out_strategyBranch': '16002,24001,f0004,f0005', 'court_ent_admi_vio': 0, 'court_ent_judge': 16, 'court_ent_trial_proc': 33, 'court_ent_tax_pay': 0, 'court_ent_owed_owe': 0, 'court_ent_tax_arrears': 0, 'court_ent_dishonesty': 0, 'court_ent_limit_entry': 0, 'court_ent_high_cons': 0, 'court_ent_pub_info': 0, 'court_ent_cri_sus': 0, 'court_ent_tax_arrears_amt_3y': 0, 'court_ent_pub_info_amt_3y': 0, 'court_ent_admi_vio_amt_3y': 0, 'court_ent_judge_amt_3y': 3978444.65, 'court_ent_docu_status': 2, 'court_ent_proc_status': 2, 'court_ent_fin_loan_con': 0, 'court_ent_loan_con': 0, 'court_ent_pop_loan': 0, 'court_ent_pub_info_max': 0, 'court_ent_judge_max': 1538321.0, 'court_ent_tax_arrears_max': 0, 'court_ent_admi_violation_max': 0, 'com_bus_status': 1, 'com_bus_endtime': '2045-09-22', 'com_bus_relent_revoke': 0, 'com_bus_case_info': 0, 'com_bus_shares_frost': 0, 'com_bus_shares_frost_his': 0, 'com_bus_shares_impawn': 0, 'com_bus_shares_impawn_his': 0, 'com_bus_mor_detail': 0, 'com_bus_mor_detail_his': 0, 'com_bus_liquidation': 0, 'com_bus_exception': 0, 'com_bus_exception_his': 0, 'com_bus_illegal_list': 0, 'com_bus_illegal_list_his': 0, 'com_bus_registered_capital': 12985.14, 'com_bus_openfrom': '2015-09-23', 'com_bus_ent_type': '有限责任公司（自然人投资或控股）', 'com_bus_esdate': '2015-09-23', 'com_bus_industryphycode': 'J', 'com_bus_areacode': '310115', 'com_bus_industrycode': '6940', 'com_bus_saicChanLegal_5y': 1, 'com_bus_saicChanInvestor_5y': 4, 'com_bus_saicChanRegister_5y': 3, 'com_bus_saicAffiliated': 3, 'com_bus_province': '上海', 'com_bus_city': '上海市', 'com_bus_leg_not_shh': 0, 'com_bus_exception_result': 0, 'com_bus_saicChanRunscope': 0, 'com_bus_legper_relent_revoke': 0, 'com_bus_legper_outwardCount1': 7, 'com_bus_industryphyname': '金融业', 'com_bus_court_open_admi_violation': 2, 'com_bus_court_open_judge_docu': 33, 'com_bus_court_open_judge_proc': 35, 'com_bus_court_open_tax_pay': 0, 'com_bus_court_open_owed_owe': 0, 'com_bus_court_open_tax_arrears': 0, 'com_bus_court_open_court_dishonesty': 0, 'com_bus_court_open_rest_entry': 0, 'com_bus_court_open_high_cons': 0, 'com_bus_court_open_pub_info': 0, 'com_bus_court_open_cri_sus': 0, 'com_bus_court_open_fin_loan_con': 0, 'com_bus_court_open_loan_con': 0, 'com_bus_court_open_pop_loan': 0, 'com_bus_court_open_pub_info_max': 0, 'com_bus_court_open_judge_max': 650259.0, 'com_bus_court_open_tax_arrears_max': 0, 'com_bus_court_open_admi_violation_max': 0.0, 'com_bus_court_open_docu_status': 2, 'com_bus_court_open_proc_status': 2, 'com_bus_face_outwardindusCode1': 'I,O', 'com_bus_face_outwardindusCount1': 2, 'base_date': '2019-08-19', 'base_idno': '91310115358479298M', 'base_gender': 0, 'base_age': 0, 'base_black': 0, 'base_type': 'COMPANY'}}}}})
    strategy_resp = strategy_response.json()
    error = jsonpath(strategy_resp, '$..Error')
    print(strategy_resp)
    print(error)

def test_to_strategy_second():
    # 调用决策引擎
    strategy_response = requests.post(STRATEGY_URL, json={
        'StrategyOneRequest': {'Header': {'InquiryCode': 'Q360763137919713280', 'ProcessCode': 'Level1_m'}, 'Body': {
            'Application': {'Variables': {
    'score_black_a1': 0,
    'score_credit_a1': 16,
    'score_debit_a1': 31,
    'score_fraud_a1': 30,
    'score_a1': 56,
    'score_black_a2': 0,
    'score_credit_a2': 16,
    'score_debit_a2': 31,
    'score_fraud_a2': 30,
    'score_a2': 56,
    'score_black_a3': 0,
    'score_credit_a3': 16,
    'score_debit_a3': 31,
    'score_fraud_a3': 30,
    'score_a3': 56,
    'score_black_c1': 0,
    'score_business_c1': 10,
    'score_c1': 56,
    'score_black_c2': 0,
    'score_business_c2': 10,
    'score_c2': 56,
    'score_black_c3': 0,
    'score_business_c3': 10,
    'score_c3': 56,
    'score_black_c4': 0,
    'score_business_c4': 10,
    'score_c4': 56,
'u_industryphycode':3,
'base_type':'UNION'
}}}}})
    strategy_resp = strategy_response.json()
    error = jsonpath(strategy_resp, '$..Error')
    print(strategy_resp)
    print(error)

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
