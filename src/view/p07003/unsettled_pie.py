from mapping.grouped_tranformer import GroupedTransformer, invoke_each


class UnsettledPie(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "unsettled_pie"

    def __init__(self, df = None) -> None:
        super().__init__()
        self.variables = {
            "debt_bal": 0,
            "debt_cnt": 0,
            "inst_pie_name": [],
            "inst_pie_loan_bal": [],
            "inst_pie_open_bal": [],
            "inst_pie_debt_prop": [],
            "bus_pie_type": [],
            "bus_pie_debt_bal": [],
            "bus_pie_debt_prop": [],
            "amt_pie": []
        }
        self.df = df


    def transform(self):

        df = self.df

