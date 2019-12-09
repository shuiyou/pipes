BASE_TYPE_TO_RELATION = [
    [
        {
            "baseType": "U_C_S_C"
        },
        {
            "userType": "COMPANY",
            "relation": "CONTROLLER"
        },
        {
            "userType": "PERSONAL",
            "relation": "SPOUSE"
        }
    ],
    [
        {
            "baseType": "U_COMPANY"
        },
        {
            "userType": "COMPANY",
            "relation": "CONTROLLER"
        }
    ],
    [
        {
            "baseType": "U_H_COMPANY"
        },
        {
            "userType": "COMPANY",
            "relation": "SHARE_HOLDER",
            "ratioMin": "0.5",
            "ratioMax": "1.0"
        }
    ]
]


def base_type_mapping(subject, parents):

    s_type = subject["userType"]
    s_relation = subject["relation"]
    for type_to_relations in BASE_TYPE_TO_RELATION:
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
