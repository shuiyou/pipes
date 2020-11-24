from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd
import numpy as np

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

        data = self.df[~self.df.bus_type.str.contains("贴现")]
        # data['margin_ratio']  = data['margin_ratio'].fillna(0)
        data['margin_ratio_c'] = 1 - data['margin_ratio'].fillna(0)
        self.variables["debt_bal"] = np.dot(data.cur_bal.tolist(),
                                            data.margin_ratio_c.tolist())
        self.variables["debt_cnt"] = data.shape[0]

        self.variables["inst_pie_name"]  = set(data.inst_name.tolist())
        inst_pie_loan_bal = []
        inst_pie_open_bal = []
        for inst in self.variables["inst_pie_name"]:
            inst_pie_loan_bal.append(
                data[(data.inst_name == inst)
                     & (pd.isnull(data.margin_ratio))].cur_bal.sum()
            )

            temp_df = data[(data.inst_name == inst)
                        &(pd.notnull(data.margin_ratio))][['cur_bal','margin_ratio_c']]
            inst_pie_open_bal.append(
                np.dot(temp_df.cur_bal.tolist(),
                       temp_df.margin_ratio_c.tolist())
            )
        self.variables["inst_pie_loan_bal"] = inst_pie_loan_bal
        self.variables["inst_pie_open_bal"] = inst_pie_open_bal
        self.variables["inst_pie_debt_prop"] = list(
            (np.array(inst_pie_loan_bal) + np.array(inst_pie_open_bal)) / self.variables["debt_bal"]
        )

        self.variables["bus_pie_type"]  = set(data.bus_type.tolist())
        bus_pie_debt_bal = []
        for biz in self.variables["bus_pie_type"]:
            temp_df = data[data.bus_type == biz][['cur_bal','margin_ratio_c']]
            bus_pie_debt_bal.append(
                np.dot(temp_df.cur_bal.tolist(),
                       temp_df.margin_ratio_c.tolist())
            )

        self.variables["bus_pie_debt_prop"] = list(
            np.array(bus_pie_debt_bal) / self.variables["debt_bal"]
        )