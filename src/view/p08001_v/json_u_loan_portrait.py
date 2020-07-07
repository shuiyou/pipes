from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df


class JsonUnionLoanPortrait(TransFlow):

    def process(self):
        self.read_u_loan_pt()

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

        loan_type_list = ['消金', '银行', '融资租赁', '担保', '保理', '小贷', '其他金融', '民间借贷']

        json = []
        for loan in loan_type_list:
            temp_df = df[df.loan_type == loan]
            json.append("\"" + loan + "\":" +
                        temp_df.set_index('loan_type').to_json(orient='records').encode(
                            'utf-8').decode("unicode_escape") + ",")

        self.variables['trans_u_loan_portrait'] = "{" + self.connect_json(json) + "}"