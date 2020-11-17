from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class SettledTable(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "settled_table"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "inst": [],
            "coop_cnt": [],
            "grant_total": [],
            "overdued": [],
            "last_repay_form": [],
            "category": [],
            "first_coop_date": [],
            "finish_coop_date": [],
            "grant_max": [],
            "grant_min": []
        }

    def transform(self):
        pass