import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df


class JsonUnionTitle(TransFlow):

    def process(self):
        self.create_u_title()

    def create_u_title(self):

        sql1 = """
            SELECT ap.related_name AS relatedName ,
            ap.relationship AS relation,
            ac.bank AS bankName,ac.account_no AS bankAccount,ac.start_time,ac.end_time
            FROM trans_apply ap
            left join trans_account ac
            on ap.account_id = ac.id
            where ap.report_req_no =  %(report_req_no)s  and  ap.account_id is not null
        """

        account_df = sql_to_df(sql=sql1,
                               params={"report_req_no":self.reqno})
        if not account_df.empty:
            account_df['start_time'] = account_df['start_time'].apply(lambda x: x.strftime('%Y/%m/%d'))
            account_df['end_time'] = account_df['end_time'].apply(lambda x: x.strftime('%Y/%m/%d'))
            account_df['startEndDate'] = account_df['start_time'] + "—" + account_df['end_time']

        account_list = account_df.drop(columns=['start_time','end_time']).to_json(orient='records')\
                    .encode('utf-8').decode("unicode_escape")

        sql2 = '''
            select related_name as name , relationship as relation
            from trans_apply
            where report_req_no = %(report_req_no)s
        '''
        relation_df = sql_to_df(sql=sql2,
                        params={"report_req_no": self.reqno}).drop_duplicates()

        json_str = "{\"cusName\":\"" + self.cusName  \
                                + "\",\"appAmt\":" + str(self.appAmt)  \
                                + ",\"流水信息\":" + account_list \
                                + ",\"关联人\":" + relation_df.to_json(orient='records')\
                                + "}"

        self.variables["表头"] = json.loads(json_str)