from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from calendar import monthrange as m_r

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
        loan_data = self.cached_data["ecredit_loan"]
        open_data = pd.merge(self.cached_data.get("ecredit_credit_biz")[['id']],
                             self.cached_data.get("ecredit_draft_lc"),
                             left_on="id", right_on="biz_id"
                             )
        self.credit_prompt(open_data)
        self.repay_predict(loan_data,open_data)
        self.debt_history(loan_data,open_data)

    def credit_prompt(self,open_data):
        info_outline = self.cached_data['ecredit_info_outline']
        self.variables['first_loan_year'] = info_outline.ix[0,'first_loan_year']
        self.variables['history_loan_cnt'] = info_outline.ix[0, 'loan_org_num']
        self.variables['on_loan_cnt'] = info_outline.ix[0, 'remain_loan_org_num']
        self.variables['first_rr_year'] = info_outline.ix[0, 'first_repay_duty_year']

        uncleared_outline = self.cached_data['ecredit_uncleared_outline']

        self.variables['loan_total_bal'] = info_outline.ix[0, 'loan_bal'] - uncleared_outline[(uncleared_outline.loan_type == "贴现")
                              &(uncleared_outline.status_type == "合计")].balance.sum()


        open_df = open_data.fillna(0)
        open_df['deposit_rate_c'] = 1 - open_df['deposit_rate']

        self.variables['open_total_bal'] = np.dot(open_df.amount.tolist(),
                                                  open_df.deposit_rate_c.tolist())

        repay_duty_biz = self.cached_data["ecredit_repay_duty_biz"]
        self.variables['rr_total_bal'] = repay_duty_biz.balance.sum()

    def repay_predict(self,loan_data,open_data):

        loan_data['end_date'] = pd.to_datetime(loan_data['end_date'])
        open_data['end_date'] = pd.to_datetime(open_data['end_date'])
        loan_data['loan_date'] = pd.to_datetime(loan_data['loan_date'])
        loan_data['end_date_month'] = loan_data.end_date.apply(lambda x: x.strftime("%Y-%m"))
        open_data['end_date_month'] = open_data.end_date.apply(lambda x: x.strftime("%Y-%m"))
        loan_data['loan_date_month'] = loan_data.loan_date.apply(lambda x: x.strftime("%Y-%m") )

        report_date = pd.to_datetime(self.cached_data["report_time"])
        temp_month = report_date.replace(day=1)
        last_6_month = []
        loan_due_amt = []
        open_due_amt = []
        loan_flow = []
        open_flow = []
        add_issuance = []
        for i in range(1,6):
            temp_month =  temp_month - timedelta(days=1)
            last_6_month.append(temp_month.strftime("%Y-%m"))
            loan_due_amt.append(loan_data[loan_data.end_date_month == temp_month.strftime("%Y-%m")].amount.sum())
            open_due_amt.append(open_data[open_data.end_date_month == temp_month.strftime("%Y-%m")].amount.sum())
            loan_flow.append(loan_data[(loan_data.loan_date<=temp_month.date())
                                       &(loan_data.end_date>temp_month.date())].balance.sum())
            open_flow.append(open_data[(open_data.loan_date<=temp_month.date())
                                       &(open_data.end_date>temp_month.date())].balance.sum())
            add_issuance.append(loan_data[loan_data.loan_date_month == temp_month.strftime("%Y-%m")].amount.sum())

            temp_month = temp_month.replace(day = 1)


        temp_month = report_date
        future_12_month = []
        loan_f_due_amt = []
        open_f_due_amt = []
        for i in range(1,12):
            future_12_month.append(temp_month.strftime("%Y-%m"))
            loan_f_due_amt.append(loan_data[loan_data.end_date_month == temp_month.strftime("%Y-%m")].amount.sum())
            open_f_due_amt.append(open_data[open_data.end_date_month == temp_month.strftime("%Y-%m")].amount.sum())
            temp_month = temp_month.replace( day = m_r(temp_month.year,temp_month.month)[1]) + timedelta(days=1)


        self.variables["last_6_month"] = last_6_month
        self.variables["loan_due_amt"] = loan_due_amt
        self.variables["open_due_amt"] = open_due_amt
        self.variables["loan_flow"] = loan_flow
        self.variables["open_flow"] = open_flow
        self.variables["add_issuance"] = add_issuance
        self.variables["future_12_month"] = future_12_month
        self.variables["loan_f_due_amt"] = loan_f_due_amt
        self.variables["open_f_due_amt"] = open_f_due_amt

    def debt_history(self,loan_data,open_data):

        debt_history = self.cached_data["ecredit_debt_histor"][['stats_date' , 'status_type', 'balance', 'account_num']]

        debt_df = pd.concat(
            [
                debt_history[debt_history.status_type == "全部负债"].set_index(['stats_date'])[['balance', 'account_num']],
                debt_history[debt_history.status_type == "关注类负债"].set_index(['stats_date'])[['balance']],
                debt_history[debt_history.status_type == "不良类负债"].set_index(['stats_date'])[['balance']]
            ],
            axis=1,
            ignore_index=True
        ).reset_index()

        debt_cols = ['history_debt_month','total_debt','debt_cnt','care_debt','bad_debt']
        debt_df.set_axis(debt_cols,
                         axis=1,
                         inplace = True)
        info_outline = self.cached_data["ecredit_info_outline"]
        assets_outline = self.cached_data["ecredit_assets_outline"]
        uncleared_outline = self.cached_data["ecredit_uncleared_outline"]

        if not assets_outline.empty:
            account_num_now = uncleared_outline.ix[0, 'account_num']  \
                              + assets_outline.ix[0,'dispose_account_num'] \
                              + assets_outline.ix[0,'advance_account_num']
        else:
            account_num_now = uncleared_outline.ix[0, 'account_num']

        debt_df = pd.concat([pd.DataFrame(data=["截止报告日",
                                                info_outline.ix[0,'loan_bal'],
                                                account_num_now,
                                                info_outline.ix[0,'loan_special_mentioned_bal'],
                                                info_outline.ix[0,'loan_non_performing_bal']
                                                ],
                                          index=debt_cols).T,
                             debt_df],
                            ignore_index=True)
        debt_df['norm_debt'] = debt_df['total_debt'] - debt_df['care_debt'] - debt_df['bad_debt']
        debt_df['abnorm_debt_prop'] = (debt_df['care_debt'] + debt_df['bad_debt'] ) / debt_df['total_debt']

        temp_df = pd.concat([ loan_data[['account_org','loan_date','end_date']],
                           open_data[['account_org','loan_date','end_date']] ],
                          ignore_index= True)
        temp_df['loan_date'] = pd.to_datetime(temp_df['loan_date'])
        temp_df['end_date'] = pd.to_datetime(temp_df['end_date'])

        inst_cnt = []
        for i in debt_df.history_debt_month.tolist():
            if i == "截止报告日":
                inst_cnt.append(self.variables["on_loan_cnt"])
            else:
                temp_date = pd.to_datetime(i)
                temp_date = temp_date.replace(day=m_r(temp_date.year, temp_date.month)[1])
                inst_cnt.append(temp_df[(temp_df.loan_date<=temp_date)
                                        &(temp_df.end_date > temp_date)].account_org.nunique())

        self.variables["history_debt_month"] = debt_df.history_debt_month.tolist()
        self.variables["total_debt"] = debt_df.total_debt.tolist()
        self.variables["norm_debt"] = debt_df.norm_debt.tolist()
        self.variables["care_debt"] = debt_df.care_debt.tolist()
        self.variables["bad_debt"] = debt_df.bad_debt.tolist()
        self.variables["abnorm_debt_prop"] = debt_df.abnorm_debt_prop.tolist()
        self.variables["debt_cnt"] = debt_df.debt_cnt.tolist()
        self.variables["inst_cnt"] = inst_cnt

