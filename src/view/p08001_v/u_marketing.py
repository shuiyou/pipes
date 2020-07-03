from view.TransFlow import TransFlow
import pandas as pd


class u_marketing(TransFlow):

    def read_u_marketing_process(self):



        def create_json( oppo_type , order ):

            df1 = df[(df.opponent_type == oppo_type) & (pd.notnull(df[order]))][[order, 'opponent_name',
                                                                                   'trans_amt', 'phone']]

            df1_1 = df1.groupby([order, 'opponent_name'])['trans_amt'] \
                .agg(['count', 'sum']).reset_index() \
                .rename(columns={'count': 'trans_cnt', 'sum': 'trans_amt'})

            df1_2 = df1[[order, 'opponent_name', 'phone']].drop_duplicates() \
                .groupby([order, 'opponent_name'])['phone'] \
                .apply(lambda x: x.str.cat(sep=';')).reset_index()

            df1 = pd.merge(df1_1, df1_2, how='left', on=[order, 'opponent_name'])
            # order列 直接是 varchar
            # df1[order] = df1[order].apply(lambda x: "No." + x  )
            # order列 是int
            df1[order] = df1[order].apply(lambda x: "No." + str(x)[0]  )

            return df1.to_json(orient='records').encode('utf-8').decode("unicode_escape")

        df = self.cached_data['trans_u_flow_portrait']
        json1 = "\"对私进账\":" + create_json(oppo_type=1, order='income_cnt_order') + ","
        json3 = "\"对私出账\":" + create_json(oppo_type=1, order='expense_cnt_order') + ","
        json2 = "\"对公进账\":" + create_json(oppo_type=2, order='income_cnt_order') + ","
        json4 = "\"对公出账\":" + create_json(oppo_type=2, order='expense_cnt_order')

        self.variables['营销反哺'] = "{" + json1 + json2 + json3 + json4 + "}"