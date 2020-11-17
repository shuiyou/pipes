from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class EcFinPress(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "ec_fin_press"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "first_loan_year": "",
            "history_loan_cnt": 0,
            "on_loan_cnt": 0,
            "first_rr_year": "",
            "loan_total_bal": 0,
            "open_total_bal": 0,
            "rr_total_bal": 0,
            "last_6_month": [],
            "loan_due_amt": [],
            "open_due_amt": [],
            "loan_flow": [],
            "open_flow": [],
            "add_issuance": [],
            "future_12_month": [],
            "loan_f_due_amt": [],
            "open_f_due_amt": [],
            "history_debt_month": [],
            "total_debt": [],
            "norm_debt": [],
            "care_debt": [],
            "bad_debt": [],
            "abnorm_debt_prop": [],
            "debt_cnt": [],
            "inst_cnt": []
        }

    def transform(self):
        pass