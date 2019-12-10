import json

import pytest
from jsonpath import jsonpath

from service.base_type_service import BaseTypeService


@pytest.fixture(name="query_data")
def base_type_query_data():
    f = open("../resource/base_type_test_01.json")
    str_json = f.read()
    json_obj = json.loads(str_json)
    query_data = jsonpath(json_obj, "$.queryData")[0]
    yield query_data


@pytest.fixture(name="query_data_02")
def base_type_query_data_02():
    f = open("../resource/base_type_test_02.json")
    str_json = f.read()
    json_obj = json.loads(str_json)
    query_data = jsonpath(json_obj, "$.queryData")[0]
    yield query_data


def test_print_subject_base_type(query_data):
    base_type_service = BaseTypeService(query_data)
    for subject in query_data:
        base_type = base_type_service.parse_base_type(subject)
        print("subject:", subject["name"], " base_type:", base_type)


def test_print_subject_base_type_02(query_data_02):
    base_type_service = BaseTypeService(query_data_02)
    for subject in query_data_02:
        base_type = base_type_service.parse_base_type(subject)
        print("subject:", subject["name"], " base_type:", base_type)