from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd

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
            "bus_sup":[],
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
        self.unsettled_table_df = None

    def transform(self):
        col_list = [
            'inst_name',
            'grant_type',
            'bus_type',
            'bus_sup',
            'guar_type',
            'grant_amt',
            'cur_bal',
            'margin_ratio',
            'start_date',
            'due_date',
            'category',
            'spec_trans',
            'spec_remark',
            'chased',
            'overdued',
            'overdue_amt',
            'overdue_prin',
            'overdue_m_cnt',
            'recent_repay_date',
            'recent_repay_amt'
        ]
        df =  pd.DataFrame(data=None,
                           columns=col_list)
        loan_data = self.cached_data["ecredit_loan"][['account_org','occur_type','account_type','biz_type','loan_guarantee_type',
                                                  'amount','balance','loan_date','end_date','category',
                                                  'special_briefgv','overdue_amt','overdue_principal',
                                                  'overdue_period','last_repay_date','last_repay_amt']]
        rename_loan = {
            'account_org':'inst_name',
            'occur_type':'grant_type',
            'account_type':'bus_sup',
            'biz_type':'bus_type',
            'loan_guarantee_type':'guar_type',
            'amount':'grant_amt',
            'balance':'cur_bal',
            'loan_date':'start_date',
            'end_date':'due_date',
            'special_briefgv':'spec_trans',
            'overdue_principal':'overdue_prin',
            'overdue_period':'overdue_m_cnt',
            'last_repay_date':'recent_repay_date',
            'last_repay_amt':'recent_repay_amt'
        }
        loan_data.rename(columns = rename_loan,
                        replace = True)

        loan1 = loan_data[loan_data.settle_status.str.contains("被追偿业务")]
        loan1['chased'] = "被追偿"
        loan2 = loan_data[loan_data.settle_status.str.contains("未结清信贷")]

        open1 = pd.merge(self.cached_data.get("ecredit_credit_biz")[['id']],
                       self.cached_data.get("ecredit_draft_lc"),
                       left_on="id",right_on="biz_id"
                       )[['account_org','biz_type','account_type',
                          'counter_guarantee_type','amount','balance','deposit_rate',
                          'loan_date','end_date','category']]
        open1 = open1[open1.settle_status.str.contains("未结清信贷")]
        rename_open = {
            'account_org':'inst_name',
            'biz_type':'bus_type',
            'account_type':'bus_sup',
            'counter_guarantee_type':'guar_type',
            'amount':'grant_amt',
            'balance':'cur_bal',
            'deposit_rate':'margin_ratio',
            'loan_date':'start_date',
            'end_date':'due_date'
        }
        open1.rename(columns = rename_open,
                     inpalce = True)

        df = pd.concat([df,loan2,loan1,open1] , ignore_index = True)
        df['overdued'] = df.overdue_amt.apply(lambda x :  "逾期" if x>0 else None)

        self.variables['inst_name'] = df.inst_name.tolist()
        self.variables['grant_type'] = df.grant_type.tolist()
        self.variables['bus_type'] = df.bus_type.tolist()
        self.variables['bus_sup'] = df.bus_sup.tolist()
        self.variables['guar_type'] = df.guar_type.tolist()
        self.variables['grant_amt'] = df.grant_amt.tolist()
        self.variables['cur_bal'] = df.cur_bal.tolist()
        self.variables['margin_ratio'] = df.margin_ratio.tolist()
        self.variables['start_date'] = df.start_date.tolist()
        self.variables['due_date'] = df.due_date.tolist()
        self.variables['category'] = df.category.tolist()
        self.variables['spec_trans'] = df.spec_trans.tolist()
        self.variables['spec_remark'] = df.spec_remark.tolist()
        self.variables['chased'] = df.chased.tolist()
        self.variables['overdued'] = df.overdued.tolist()
        self.variables['overdue_amt'] = df.overdue_amt.tolist()
        self.variables['overdue_prin'] = df.overdue_prin.tolist()
        self.variables['overdue_m_cnt'] = df.overdue_m_cnt.tolist()
        self.variables['recent_repay_date'] = df.recent_repay_date.tolist()
        self.variables['recent_repay_amt'] = df.recent_repay_amt.tolist()

        self.unsettled_table_df = df
