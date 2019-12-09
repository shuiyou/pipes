BASE_TYPE_MAPPING_ORIGIN_DATA = [
    "U_C_S_C        >>>     userType:COMPANY & relation:CONTROLLER      >>>   userType:PERSONAL & relation:SPOUSE",
    "U_COMPANY      >>>     userType:COMPANY & relation:CONTROLLER",
    "U_COMPANY      >>>     userType:COMPANY & relation:CONTROLLER",
    "U_H_COMPANY    >>>     userType:COMPANY & relation:SHARE_HOLDER & ratioMin:0.5 & ratioMax:1.0",
]


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


BASE_TYPE_MAPPING = arrow_dict_to_array(BASE_TYPE_MAPPING_ORIGIN_DATA)


def base_type_mapping(subject, parents):
    s_type = subject["userType"]
    s_relation = subject["relation"]
    for type_to_relations in BASE_TYPE_MAPPING:
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
