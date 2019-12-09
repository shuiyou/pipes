import json

from jsonpath import jsonpath

from util.base_type_mapping import base_type_mapping


class BaseTypeService(object):
    def __init__(self, file_path):
        f = open(file_path)
        str_json = f.read()
        json_obj = json.loads(str_json)
        self.query_data = jsonpath(json_obj, "$.strategyParam.queryData")[0]

    def print_subject_base_type(self):
        for subject in self.query_data:
            self._parse_base_type(subject)

    def _parse_base_type(self, subject):
        parents = []
        self.fetch_parents(subject, self.query_data, parents)
        base_type = base_type_mapping(subject, parents)
        print("subject:", subject["name"], " base_type:", base_type)

    def fetch_parents(self, subject, query_data, parents):
        parent_id = subject["parentId"]
        if parent_id is None or parent_id == 0:
            return

        for sub in query_data:
            if sub["id"] == parent_id:
                parents.append(sub)
                self.fetch_parents(sub, query_data, parents)


def test_parse_base_type():
    service = BaseTypeService("../resource/base_type_test_01.json")
    print("Init base type service finished.")
    service.print_subject_base_type()


