import json

from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df

class JsonUnionGuarantor(TransFlow):

    def process(self):
        self.read_guarantor_in_u_flow()

    def create_guarantor_json(self,guarantor):
        sql = """
                    select concat(trans_date," ",trans_time) as trans_time,
                    opponent_name,trans_amt,remark,is_before_interest_repay,
                    income_amt_order,expense_amt_order,income_cnt_order,expense_cnt_order
                    from trans_u_flow_portrait
                    where report_req_no = %(report_req_no)s
                """
        df = sql_to_df(sql=sql,
                       params={"report_req_no": self.reqno})
        df = df[df.opponent_name == guarantor]
        df.rename(columns={'opponent_name': 'guarantor'}, inplace=True)

        json1 = "\"流水\":" + df[['guarantor', 'trans_amt',
                                'trans_time', 'remark',
                                'is_before_interest_repay']].to_json(orient='records').encode('utf-8').decode(
            "unicode_escape") + ","

        income_df = df[df.trans_amt > 0][['guarantor', 'trans_amt']]
        expense_df = df[df.trans_amt < 0][['guarantor', 'trans_amt']]

        income_df = income_df.groupby(['guarantor'])['trans_amt'].agg(['count', 'sum']) \
            .reset_index().rename(columns={'count': 'income_cnt', 'sum': 'income_amt'})
        expense_df = expense_df.groupby(['guarantor'])['trans_amt'].agg(['count', 'sum']) \
            .reset_index().rename(columns={'count': 'expense_cnt', 'sum': 'expense_amt'})

        df_ = pd.merge(income_df, expense_df, how='left', on='guarantor')

        df_ = pd.merge(df_, df[['guarantor', 'income_amt_order', 'expense_amt_order',
                                'income_cnt_order', 'expense_cnt_order']].drop_duplicates(),
                       how='left', on='guarantor')

        json2 = "\"提示\":" + df_.to_json(orient='records').encode('utf-8').decode("unicode_escape")[1:-1]
        return "{" + json1 + json2 + "},"


    def read_guarantor_in_u_flow(self):

        # 获取guarantor 一个/多个担保人姓名 列表
        guarantor_list = []
        json_str = ""
        for guarantor in guarantor_list:
            json_str += self.create_guarantor_json(guarantor)

        self.variables['第三方担保交易信息'] = json.loads("[" + json_str[:-1] + "]")