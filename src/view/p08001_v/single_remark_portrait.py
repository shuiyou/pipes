from view.TransFlow import TransFlow
import pandas as pd


class trans_single_remark_portrait(TransFlow):

    def read_single_remark_pt_process(self):


        df = self.cached_data['trans_single_remark_portrait']
        df.drop(columns = ['id','account_id','report_req_no','create_time','update_time'],
                                    inplace = True)
        # income_order_max = df.remark_income_amt_order.max()
        # expense_order_max = df.remark_expense_amt_order.max()

        income_df = df[pd.notnull(df['remark_income_amt_order'])].drop(columns='remark_expense_amt_order')
        income_df.rename(columns={'remark_income_amt_order': 'order'}, inplace=True)
        json1 = income_df.to_json(orient = 'records').encode('utf-8').decode("unicode_escape")

        expense_df = df[pd.notnull(df['remark_expense_amt_order'])].drop(columns='remark_income_amt_order')
        expense_df.rename(columns={'remark_expense_amt_order': 'order'}, inplace=True)
        json2 = expense_df.to_json(orient = 'records').encode('utf-8').decode("unicode_escape")

        self.variables['trans_single_remark_portrait'] = "{\"remark_income\":" + json1 + ","\
                                                        + "\"remark_expense\":" + json2 + "}"