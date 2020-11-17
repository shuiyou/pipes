from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class UnsettledTable(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "unsettled_table"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "inst_name": [],
            "grant_type": [],
            "bus_type": [],
            "guar_type": [],
            "grant_amt": [],
            "cur_bal": [],
            "margin_ratio": [],
            "start_date": [],
            "due_date": [],
            "category": [],
            "spec_trans": [],
            "spec_remark": [],
            "chased": [],
            "overdued": [],
            "overdue_amt": [],
            "overdue_prin": [],
            "overdue_m_cnt": [],
            "recent_repay_date": [],
            "recent_repay_amt": []
        }

    def transform(self):
        pass