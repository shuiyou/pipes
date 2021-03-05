import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df


class JsonUnionLoanPortrait(TransFlow):

    def process(self):
        self.read_u_loan_pt()
        self.read_u_loan_trans_detail()

    def connect_json(self, json):
        string = ''
        for text in json:
            string += text
        return string[:-1]

    def read_u_loan_pt(self):
        sql = """
            select *
            from trans_u_loan_portrait
            where report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"report_req_no": self.reqno})

        df.drop(columns=['id', 'apply_no', 'report_req_no', 'create_time', 'update_time'],
                inplace=True)

        loan_type_list = [  '消金',
                            '融资租赁',
                            '担保',
                            '保理',
                            '小贷',
                            '银行',
                            '第三方支付',
                            '其他金融',
                            '民间借贷']

        json_str = []
        for loan in loan_type_list:
            temp_df = df[df.loan_type == loan]
            json_str.append("\"" + loan + "\":" +
                        temp_df.set_index('loan_type').to_json(orient='records').encode(
                            'utf-8').decode("unicode_escape") + ",")

        self.variables['trans_u_loan_portrait'] = json.loads("{" + self.connect_json(json_str) + "}")

    def read_u_loan_trans_detail(self):

        sql = """
            select bank,account_no,concat(trans_date," ",trans_time) as trans_time,opponent_name,trans_amt,remark,loan_type
            from trans_u_flow_portrait
            where report_req_no = %(report_req_no)s and loan_type is not null
        """
        flow_df = sql_to_df(sql=sql,
                            params={"report_req_no": self.reqno})

        flow_df['account_no'] = flow_df['account_no'].fillna("").astype(str)
        flow_df['account_no'] = flow_df['account_no'].apply(lambda x: self.flow_account_clean(x))

        if flow_df.empty:
            return
        json_str = []
        loan_type_list = ['消金',
                          '融资租赁',
                          '担保',
                          '保理',
                          '小贷',
                          '银行',
                          '第三方支付',
                          '其他金融',
                          '民间借贷']
        for loan in loan_type_list:
            temp_df = flow_df[flow_df.loan_type == loan].drop(columns=["loan_type"])
            json_str.append("\"" + loan + "\":" +
                            temp_df.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ",")

        self.variables["多头明细"] = json.loads("{" + self.connect_json(json_str) + "}")