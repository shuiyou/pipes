import json

from view.TransFlow import TransFlow
import pandas as pd
from util.mysql_reader import sql_to_df

class JsonSingleRemarkTransDetail(TransFlow):

    def process(self):
        self.read_single_remark_trans_in_flow()

    def connect_json( self, json ):
        string = ''
        for text in json :
            string += text
        return string[:-1]


    def read_single_remark_trans_in_flow(self):

        sql1 = """
            select concat(trans_date," ",trans_time) as trans_time,opponent_name,trans_amt,remark
            from trans_flow_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        """
        flow_df = sql_to_df(sql=sql1, params={"account_id": self.account_id,
                                                "report_req_no":self.reqno})

        sql2 = """
            select *
            from trans_single_remark_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        """

        remark_portrait = sql_to_df( sql = sql2,
                                     params={"account_id": self.account_id,
                                               "report_req_no":self.reqno})
        if flow_df.empty or remark_portrait.empty:
            return

        remark_portrait.drop(columns=['id','account_id','report_req_no','create_time','update_time'],
                             inplace = True)

        remark_income_dict = remark_portrait[pd.notnull(remark_portrait.remark_income_amt_order)] \
            [['remark_income_amt_order', 'remark_type']]. \
            set_index('remark_income_amt_order')['remark_type'].to_dict()

        json1 = []
        for i in remark_income_dict:
            # order列应为int
            # i = int(i)
            temp_df = flow_df[(flow_df.remark == remark_income_dict[i]) & (flow_df.trans_amt > 0)]. \
                rename(columns={'opponent_name': 'oppo_name'})
            key = str(i).replace(".", "_")
            json1.append((f"\"{key}\"" + ":" + temp_df.to_json(orient='records')) + ",")

        json_1 = self.connect_json(json1).encode('utf-8').decode("unicode_escape")

        remark_expense_dict = remark_portrait[pd.notnull(remark_portrait.remark_expense_amt_order)] \
            [['remark_expense_amt_order', 'remark_type']]. \
            set_index('remark_expense_amt_order')['remark_type'].to_dict()

        json2 = []
        for j in remark_expense_dict:
            # order列应为int
            # j = int(j)
            temp_df = flow_df[(flow_df.remark == remark_expense_dict[j]) & (flow_df.trans_amt < 0)]. \
                rename(columns={'opponent_name': 'oppo_name'})
            key = str(j).replace(".", "_")
            json2.append((f"\"{key}\"" + ":" + temp_df.to_json(orient='records')) + ",")

        json_2 = self.connect_json(json2).encode('utf-8').decode("unicode_escape")

        json_str = "{\"remark_income_amt_order\":{"+ json_1 + \
                                   "},\"remark_expense_amt_order\":{" + json_2 + "}}"

        self.variables["交易对手明细"] = json.loads(json_str)