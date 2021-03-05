import json

from view.TransFlow import TransFlow
from util.mysql_reader import sql_to_df
import pandas as pd

class JsonUnionTitle(TransFlow):

    def process(self):
        self.create_u_title()

    def create_u_title(self):

        sql1 = """
            SELECT ap.related_name AS relatedName ,
            ap.relationship AS relation,
            ac.bank AS bankName,ac.account_no AS bankAccount,
            min(acc.start_time) as start_time, max(acc.end_time) as end_time
            FROM trans_apply ap
            left join trans_account ac
            on ap.account_id = ac.id
            left join trans_account acc
            on ac.account_no = acc.account_no
            where ap.report_req_no =  %(report_req_no)s  and  ap.account_id is not null
            group by relatedName,relation,bankName,bankAccount
        """

        account_df = sql_to_df(sql=sql1,
                               params={"report_req_no":self.reqno})
        if not account_df.empty:
            account_df['start_time'] = account_df['start_time'].apply(lambda x: x.strftime('%Y/%m/%d'))
            account_df['end_time'] = account_df['end_time'].apply(lambda x: x.strftime('%Y/%m/%d'))
            account_df['startEndDate'] = account_df['start_time'] + "—" + account_df['end_time']

        cashier = pd.DataFrame(data=None,
                               columns=['name', 'bank', 'account'])
        for account in self.cached_data.get('input_param'):
            if str(account).__contains__('\'ifCashier\': \'是\''):
                cashier.loc[len(cashier)] = [account.get('name'),
                                             account.get('extraParam').get('accounts')[0]['bankName'],
                                             account.get('extraParam').get('accounts')[0]['bankAccount']]

        if not cashier.empty:
            cashier['account_detail'] = '(出纳)'
            account_df = pd.merge(account_df,cashier,
                                  how = 'left',
                                  left_on=['relatedName','bankName','bankAccount'],
                                  right_on=['name', 'bank', 'account']).fillna("")
            account_df['bankAccount'] = account_df['bankAccount'] + account_df['account_detail']


        account_list = account_df[['relatedName','relation','bankName','bankAccount','startEndDate']].to_json(orient='records')\
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