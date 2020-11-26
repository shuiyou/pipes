from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd

class SettledTable(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "settled_table"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            "inst": [],
            "coop_cnt": [],
            "grant_total": [],
            "overdued": [],
            "last_repay_form": [],
            "category": [],
            "first_coop_date": [],
            "finish_coop_date": [],
            "grant_max": [],
            "grant_min": []
        }

    def transform(self):

        loan_total = self.cached_data["ecredit_loan"][['id','settle_status','account_org','amount','loan_date','end_date',
                                                      'category','last_payment_type']]
        loan_data = loan_total[loan_total.settle_status.str.contains("已结清信贷")].drop(columns='settle_status')

        loan_data['loan_date'] = pd.to_datetime(loan_data['loan_date'])

        group1 = loan_data.drop(columns = 'id').groupby('account_org').agg({'amount':['count','sum','max','min'],
                                                       'loan_date':['min','max']})
        group1.set_axis(['coop_cnt', 'grant_total',
                         'grant_max', 'grant_min',
                         'first_coop_date', 'finish_coop_date'],
                           axis='columns',
                           inplace=True)

        group2 = loan_data.drop(columns = 'id').sort_values( by = ['account_org','loan_date'] ,ascending = [True,False]  )\
            .drop_duplicates(subset = 'account_org',keep='first')[['account_org','last_payment_type','category']].set_index('account_org')

        group3 = pd.merge(loan_data[['id']],
                       self.cached_data["ecredit_histor_perfo"],
                       how='left',
                       left_on='id',
                       right_on='loan_id').groupby('account_org')['overdue_amt'].agg('sum').to_frame()
        group3['overdue_amt'] = group3.overdue_amt.apply( lambda x : "是" if x > 0 else None)

        df = pd.concat( [group1 , group2 , group3] ,axis=1).reset_index()
        df.rename(columns = { 'account_org' : 'inst',
                              'overdue_amt' : 'overdued',
                              'last_payment_type':'last_repay_form'},
                  inplace = True)

        loan_list = loan_total[loan_total.settle_status.str.contains("被追偿|未结清")]['account_org'].drop_duplicates().tolist()

        df['finish_coop_date'] = df.finish_coop_date.apply(lambda x : "在贷" if x in loan_list else x)

        self.variables["inst"] = df.inst.tolist()
        self.variables["coop_cnt"] = df.coop_cnt.tolist()
        self.variables["grant_total"] = df.grant_total.tolist()
        self.variables["overdued"] = df.overdued.tolist()
        self.variables["last_repay_form"] = df.last_repay_form.tolist()
        self.variables["category"] = df.category.tolist()
        self.variables["first_coop_date"] = df.first_coop_date.tolist()
        self.variables["finish_coop_date"] = df.finish_coop_date.tolist()
        self.variables["grant_max"] = df.grant_max.tolist()
        self.variables["grant_min"] = df.grant_min.tolist()