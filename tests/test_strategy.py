import json

from logger.logger_util import LoggerUtil
from strategy.request import request_from_dict, Header, StrategyOneRequest, empty_instance
from strategy.response import Response

LoggerUtil().logger(__name__)


def test_request():
    stategy_request = empty_instance()
    stategy_request.strategy_one_request.header.process_code = 'CJR'
    print(json.dumps(stategy_request.to_dict()))


def test_response():
    Response.from_dict({
        "StrategyOneResponse": {
            "Header": {
                "InquiryCode": "a855b0c78cd146688b33441769c6cbb4",
                "ProcessCode": "MYD",
                "OrganizationCode": "",
                "ProcessVersion": 4,
                "LayoutVersion": 1
            },
            "Body": {
                "Application": {
                    "Variables": {
                        "out_isQuery": "N",
                        "out_strategyBranch": "90000000",
                        "out_result": "A",
                        "td_risk_tel_hit_high_att": 0,
                        "risk_score_level1": 5,
                        "temp_hd_score": 1.12,
                        "temp_hf_score": 0,
                        "temp_jxl_score": 35.88,
                        "temp_qh_score": 0,
                        "temp_td_score": 1.2,
                        "temp_hf_other_max_level": 0,
                        "temp_model_score": 5,
                        "temp_risk_native": 1,
                        "temp_hf_pub_info_max_level": 0,
                        "temp_hf_tax_arrears_max_level": 0
                    },
                    "Categories": [
                        {
                            "Reason": {
                                "Variables": {
                                    "out_decisionBranchCode": "K202",
                                    "out_rejectReasonCode": "企业法院案件",
                                    "out_riskReasonCode": "企业法院案件"
                                }
                            }
                        },
                        {
                            "Reason": {
                                "Variables": {
                                    "out_decisionBranchCode": "K203",
                                    "out_rejectReasonCode": "企业法院案件",
                                    "out_riskReasonCode": "企业法院案件"
                                }
                            }
                        },
                        {
                            "Reason": {
                                "Variables": {
                                    "out_decisionBranchCode": "K210",
                                    "out_rejectReasonCode": "企业法院案件",
                                    "out_riskReasonCode": "企业法院案件"
                                }
                            }
                        }
                    ]
                }
            }
        }
    })
