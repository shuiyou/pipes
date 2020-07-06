from view.TransFlow import TransFlow
import pandas as pd


class JsonUnionRemarkPortrait(TransFlow):

    def process(self):
        self.read_u_remark_pt()

    def read_u_remark_pt(self):

        df = self.cached_data['trans_u_remark_portrait']
        df.drop(columns=['id', 'apply_no', 'report_req_no', 'create_time', 'update_time'],
                inplace=True)

        income_df = df[pd.notnull(df['remark_income_amt_order'])].drop(columns='remark_expense_amt_order')
        income_df.rename(columns={'remark_income_amt_order': 'order'}, inplace=True)
        json1 = income_df.to_json(orient='records').encode('utf-8').decode("unicode_escape")

        expense_df = df[pd.notnull(df['remark_expense_amt_order'])].drop(columns='remark_income_amt_order')
        expense_df.rename(columns={'remark_expense_amt_order': 'order'}, inplace=True)
        json2 = expense_df.to_json(orient='records').encode('utf-8').decode("unicode_escape")

        self.variables["trans_u_remark_portrait"] = "{\"remark_income\":" + json1 + "," \
                                                         + "\"remark_expense\":" + json2 + "}"