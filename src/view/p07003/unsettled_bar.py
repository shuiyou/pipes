from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class UnsettledBar(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "unsettled_bar"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "guar_type": [],
            "guar_amt": [],
            "guar_bal": [],
            "guar_cnt": [],
            "bus_bar": [],
            "grant_history_bar": []
        }

    def transform(self):
        pass