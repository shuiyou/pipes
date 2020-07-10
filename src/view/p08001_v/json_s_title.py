from view.TransFlow import TransFlow
from datetime import datetime
import pandas as pd
from util.mysql_reader import sql_to_df

class JsonSingleTitle(TransFlow):

    def process(self):
        self.create_s_title()


    def create_s_title(self):

        param_list = self.cached_data.get("input_param")
        for i in param_list:
            if i["relation"] == "MAIN":
                self.cusName = i["name"]
                self.bankName = i["extraParam"]["accounts"][0]["bankName"]
                self.bankAccount = i["extraParam"]["accounts"][0]["bankAccount"]
                self.idno = i["idno"]
                self.reqno = i["preReportReqNo"]

        sql1 = '''
            SELECT min(f.trans_time) AS start_time, 
            max(f.trans_time) AS end_time
            FROM trans_account ac
            left join trans_flow f 
            on ac.id = f.account_id
            where ac.account_no = %(account_no)s
        '''

        df1 = sql_to_df(sql=sql1,
                       params={"account_no":self.bankAccount})

        start_end_date = ""
        if not df1.empty:
            start_end_date = df1.start_time.iloc[0].strftime('%Y年%m月%d日') + "——" + df1.end_time.iloc[0].strftime('%Y年%m月%d日')

        sql2 = '''
            select related_name as name , relationship as relation
            from trans_apply
            where report_req_no = %(report_req_no)s
        '''
        df2 = sql_to_df(sql=sql2,
                        params={"report_req_no": self.reqno})

        self.variables["表头"] = "{\"cusName\":" + self.cusName  \
                               + ",\"流水信息\":{\"bankName\":" + self.bankName \
                               + ",\"bankAccount\":" + self.bankAccount \
                               + ",\"startEndDate\":" + start_end_date + "},"  \
                + "\"关联人\":" + df2.to_json(orient='records').encode('utf-8').decode("unicode_escape") + "}"
