import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df

class JsonSingleLoanPortrait(TransFlow):

    def process(self):
        self.read_single_loan_pt()

    def connect_json(self,json):
        string = ''
        for text in json:
            string += text
        return string[:-1]

    def read_single_loan_pt(self):
        sql = """
            select * 
            from trans_single_loan_portrait
            where account_id = %(account_id)s and report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql = sql,
                       params= {"account_id": self.account_id,
                               "report_req_no":self.reqno})
        if df.empty:
            return

        df.drop(columns = ['id','account_id','report_req_no','create_time','update_time'],
                    inplace = True)


        loan_type_list = ['消金','银行','融资租赁','担保','保理','小贷','其他金融','民间借贷']

        json_list =[]
        for loan in loan_type_list:
            temp_df = df[df.loan_type == loan]

            json_list.append("\"" + loan + "\":" +
                        temp_df.set_index('loan_type').to_json(orient='records').encode('utf-8').decode("unicode_escape")
                        + ",")

        json_str =  "{" + self.connect_json(json_list) + "}"
        self.variables["trans_single_loan_portrait"] =json.loads(json_str)