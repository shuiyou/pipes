from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd
import numpy as np
from datetime import datetime

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
        self.credit_prompt()
        self.repay_predict()
        self.debt_history()

    def credit_prompt(self):
        info_outline = self.cached_data['ecredit_info_outline']
        self.variables['first_loan_year'] = info_outline.ix[0,'first_loan_year']
        self.variables['history_loan_cnt'] = info_outline.ix[0, 'loan_org_num']
        self.variables['on_loan_cnt'] = info_outline.ix[0, 'remain_loan_org_num']
        self.variables['first_rr_year'] = info_outline.ix[0, 'first_repay_duty_year']

        uncleared_outline = self.cached_data['ecredit_uncleared_outline']
        self.variables['loan_total_bal'] = info_outline.ix[0, 'loan_bal'] - \
            uncleared_outline[(uncleared_outline.loan_type == "贴现")
                              &(uncleared_outline.status_type == "合计")]['balance'].values[0]

        open_df = pd.merge(self.cached_data.get("ecredit_credit_biz")[['id']],
                       self.cached_data.get("ecredit_draft_lc"),
                       left_on="id",right_on="biz_id"
                       ).fillna(0)
        open_df['deposit_rate_c'] = 1 - open_df['deposit_rate']

        self.variables['open_total_bal'] = np.dot(open_df.amount.tolist(),
                                                  open_df.deposit_rate_c.tolist())

        repay_duty_biz = self.cached_data["ecredit_repay_duty_biz"]
        self.variables['rr_total_bal'] = repay_duty_biz.balance.sum()

    def repay_predict(self):
        pass

    def debt_history(self):
        pass
