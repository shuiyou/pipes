from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df

class JsonUnionSummaryPortrait(TransFlow):

    def process(self):
        self.read_u_summary_pt()


    def read_u_summary_pt(self):
        sql = """
            select *
            from trans_u_summary_portrait
            where report_req_no = %(report_req_no)s
        """
        df = sql_to_df(sql=sql,
                       params={"report_req_no": self.reqno})
        df.drop(columns=['id', 'apply_no', 'report_req_no', 'q_1_year', 'q_2_year',
                         'q_3_year', 'q_4_year', 'create_time', 'update_time'],
                inplace=True)

        self.variables["trans_u_summary_portrait"] = df.to_json(orient='records')