import json

from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df

class JsonUnionCounterpartyPortrait(TransFlow):

    def process(self):
        self.read_u_counterparty_pt()

    def connect_json(self,json):
        string = ''
        for text in json:
            string += text
        return string[:-1]

    def read_u_counterparty_pt(self):

        sql = """
            select *
            from trans_u_counterparty_portrait
            where report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"report_req_no":self.reqno})
        df.drop(columns=['id', 'apply_no', 'report_req_no', 'create_time', 'update_time'],
                inplace=True)

        income_df = df[pd.notnull(df.income_amt_order)]
        income_order_list = list(map(str, list(range(1, 11)))) + \
                            ['前5', '前10',
                             '前10%', '前20%', '前30%', '前40%', '前50%',
                             '前60%', '前70%', '前80%', '前90%', '前100%']

        json1 = []
        for income_order in income_order_list:
            temp_df1 = income_df[income_df.income_amt_order == income_order]
            json1.append("\"" + income_order +  "\":"   +
                         temp_df1.to_json( orient = 'records').encode('utf-8').decode("unicode_escape") + ",")

        # json_1 = self.connect_json(json1)

        expense_df = df[pd.notnull(df.expense_amt_order)]
        expense_order_list = list(map(str, list(range(1, 11))))

        json2 = []
        for expense_order in expense_order_list:
            temp_df2 = expense_df[expense_df.expense_amt_order == expense_order].drop(columns='expense_amt_order')
            json2.append("\"" + expense_order + "\":" +
                         temp_df2.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ",")

        # json_2 = self.connect_json(json2)

        self.variables["trans_u_counterparty_portrait"] = json.loads("{\"income_amt_order\":{"
                                                                     + self.connect_json(json1) \
                                                                      +"},\"expense_amt_order\":{" \
                                                                      + self.connect_json(json2) + "}}")