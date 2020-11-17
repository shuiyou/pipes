from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class RrTable(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "rr_table"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "inst_name": [],
            "guar_type": [],
            "r_type": [],
            "bus_type": [],
            "r_amt": [],
            "r_bal": [],
            "start_date": [],
            "due_date": [],
            "category": [],
            "left_m_cnt": [],
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