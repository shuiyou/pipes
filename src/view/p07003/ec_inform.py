from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class EcInform(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "ec_inform"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "e_name": "",
            "credict_code": "",
            "soci_credict_code": "",
            "industry": "",
            "launch_year": "",
            "address": "",
            "status": "",
            "capital": "",
            "related_name": [],
            "relation": [],
            "id_code": [],
            "update_date": [],
            "remark": []

        }

    def transform(self):
        pass