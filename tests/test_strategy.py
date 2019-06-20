import json

import jsonschema

from logger.logger_util import LoggerUtil
from strategy.request import request_schema, validate_input

LoggerUtil().logger(__name__)


def test_request():
    input = {
        "StrategyOneRequest": {
            "Header": {
                "InquiryCode": "c3ef30f0ad5646d8a25136f98532ec9f",
                "ProcessCode": "JB_WZ_CJR2"
            },
            "Body": {
                "Application": {
                    "Variables": {
                        'foo': 'bar'
                    }
                }
            }
        }
    }
    validate_input(input)


def test_replace():
    value = '1, 3'
    r = value.replace('[', '').replace(')', '')
    print(int(r.split(',')[1]))
    print(int(r.split(',')[1]) // 12)
