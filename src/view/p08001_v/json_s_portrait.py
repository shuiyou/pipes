import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df


class JsonSinglePortrait(TransFlow):

    def process(self):
        self.read_single_pt()

    def read_single_pt(self):
        sql = """
            select * 
            from trans_single_portrait
            where account_id = %(account_id)s and report_req_no=%(report_req_no)s
            """

        df = sql_to_df(sql=sql,
                       params={"account_id": self.account_id, "report_req_no": self.reqno})
        if df.empty:
            return
        df.drop(columns=['id', 'account_id', 'report_req_no', 'create_time', 'update_time'], inplace=True)
        # str_col = []
        # for col in df.columns.tolist():
        #     if col.dtype == object:
        #         str_col.append(col)
        # df.fillna(0,inplace = True)
        # for col in str_col:
        #     df[col].replace(0,'',inplace = True)
        json_str = df.to_json(orient='records')
        data = json.loads(json_str)
        self.variables["trans_single_portrait"] = data[0]


