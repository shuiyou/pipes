from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df

class JsonUnionRelatedPortrait(TransFlow):

    def process(self):
        self.read_u_related_pt()

    def read_u_related_pt(self):
        sql = """
            select *
            from trans_u_related_portrait
            where report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"report_req_no": self.reqno})
        df.drop(columns=['id', 'apply_no', 'report_req_no',
                         'income_cnt_order', 'income_amt_order',
                         'expense_cnt_order', 'expense_amt_order',
                         'create_time', 'update_time'],
                inplace=True)

        self.variables["trans_u_related_portrait"] = df.to_json(
                        orient='records').encode('utf-8').decode("unicode_escape")