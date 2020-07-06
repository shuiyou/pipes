
from fileparser.single_account_portrait.trans_flow import transform_class_str
import pandas as pd


class SingleRemarkPortrait:

    def __init__(self, trans_flow):
        self.trans_flow_portrait_df = trans_flow.trans_flow_portrait_df
        self.db = trans_flow.db
        self.variables = []

    # todo account_id
    def process(self):
        self._remark_detail()
        self.db.session.add_all(self.variables)
        self.db.session.commit()

    def _remark_detail(self):
        flow_df = self.trans_flow_portrait_df
        income_df = flow_df[(pd.notnull(flow_df.remark_type)) & (flow_df.trans_amt > 0)]
        expense_df = flow_df[(pd.notnull(flow_df.remark_type)) & (flow_df.trans_amt < 0)]
        income_group_df = income_df.groupby(by='remark_type').agg({'trans_amt': sum, 'remark_type': len})
        expense_group_df = expense_df.groupby(by='remark_type').agg({'trans_amt': sum, 'remark_type': len})
        income_group_df.sort_values(by='trans_amt', ascending=False, inplace=True)
        expense_group_df.sort_values(by='trans_amt', ascending=True, inplace=True)

        income_len = len(income_group_df)
        expense_len = len(expense_group_df)
        for i in range(10):
            income_temp = {}
            expense_temp = {}
            if i+1 < income_len:
                income_temp['remark_type'] = income_group_df.index[i]
                income_temp['remark_trans_cnt'] = income_group_df.iloc[i, 1]
                income_temp['remark_trans_amt'] = income_group_df.iloc[i, 0]
            else:
                income_temp['remark_type'] = ''
                income_temp['remark_trans_cnt'] = 0
                income_temp['remark_trans_cnt'] = 0
            if i+1 < expense_len:
                expense_temp['remark_type'] = expense_group_df.index[i]
                expense_temp['remark_trans_cnt'] = expense_group_df.iloc[i, 1]
                expense_temp['remark_trans_amt'] = expense_group_df.iloc[i, 0]
            else:
                expense_temp['remark_type'] = ''
                expense_temp['remark_trans_cnt'] = 0
                expense_temp['remark_trans_cnt'] = 0
            income_temp['remark_income_amt_order'] = i + 1
            expense_temp['remark_expense_amt_order'] = i + 1
            role1 = transform_class_str(income_temp, 'TransSingleRemarkPortrait')
            role2 = transform_class_str(expense_temp, 'TransSingleRemarkPortrait')
            self.variables.append(role1)
            self.variables.append(role2)
