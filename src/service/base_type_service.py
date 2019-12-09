# -*- coding: utf-8 -*-
# @Time : 2019/12/9 11:09 AM
# @Author : lixiaobo
# @Site : 
# @File : base_type_service.py.py
# @Software: PyCharm
import json
import threading

from resources.resource_util import read_content

BASE_TYPE_MAPPING_INIT_LOCK = threading.Lock()


class BaseTypeService(object):
    BASE_TYPE_MAPPING = None

    def __init__(self, query_data):
        self.query_data = query_data

        if BaseTypeService.BASE_TYPE_MAPPING is not None:
            return

        try:
            BASE_TYPE_MAPPING_INIT_LOCK.acquire()
            if BaseTypeService.BASE_TYPE_MAPPING is None:
                mapping_data = read_content("base_type_mapping.json")
                mapping_dicts = json.loads(mapping_data)
                BaseTypeService.BASE_TYPE_MAPPING = BaseTypeService.arrow_dict_to_array(mapping_dicts)
        finally:
            BASE_TYPE_MAPPING_INIT_LOCK.release()

    def parse_base_type(self, subject):
        parents = []
        self.fetch_parents(subject, parents)
        return BaseTypeService.base_type_mapping(subject, parents)

    def fetch_parents(self, subject, parents):
        parent_id = subject["parentId"]
        if parent_id is None or parent_id == 0:
            return

        for sub in self.query_data:
            if sub["id"] == parent_id:
                parents.append(sub)
                self.fetch_parents(sub, parents)

    @staticmethod
    def arrow_dict_to_array(origin_data_struct):
        base_type_relations = []
        for item in origin_data_struct:
            base_type_items = []
            item_sections = item.split(">>>")
            for col_index, item_section in enumerate(item_sections):
                item_info_arr = item_section.split("&")
                base_type_item = {}
                for item_info in item_info_arr:
                    if col_index == 0:
                        base_type_item["baseType"] = item_info.strip()
                    else:
                        key_val = item_info.split(":")
                        base_type_item[key_val[0].strip()] = key_val[1].strip()
                    print("item_info:", base_type_item)
                base_type_items.append(base_type_item)
            base_type_relations.append(base_type_items)
        print("base_type_relations=", base_type_relations)
        return base_type_relations

    @staticmethod
    def base_type_mapping(subject, parents):
        s_type = subject["userType"]
        s_relation = subject["relation"]
        for type_to_relations in BaseTypeService.BASE_TYPE_MAPPING:
            if len(type_to_relations) != len(parents) + 2:
                continue
            if s_type != type_to_relations[1]["userType"] or s_relation != type_to_relations[1]["relation"]:
                continue

            if "ratioMin" in type_to_relations[1] and "ratioMax" in type_to_relations[1] and "fundratio" in subject:
                fund_ratio = subject["fundratio"]
                ratioMin = float(type_to_relations[1]["ratioMin"])
                ratioMax = float(type_to_relations[1]["ratioMax"])
                if not (ratioMin <= fund_ratio < ratioMax):
                    continue

            all_match = True
            for index, type_to_relation in enumerate(type_to_relations):
                if index < 2:
                    continue
                c_type = type_to_relation["userType"]
                c_relation = type_to_relation["relation"]
                if c_type != parents[index - 2]["userType"] or c_relation != parents[index - 2]["relation"]:
                    all_match = False
            if all_match:
                return type_to_relations[0]["baseType"]
        return None
