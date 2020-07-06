from view.TransFlow import TransFlow
import pandas as pd


class JsonSingleUnusualTrans(TransFlow):

    def process(self):
        self.read_unusual_in_flow()

    def read_unusual_in_flow(self):
        df = self.cached_data['trans_flow_portrait']
        df = df[pd.notnull(df.unusual_trans_type)][['trans_date','trans_time',
                                                    'opponent_name','trans_amt',
                                                    'remark','unusual_trans_type']]

        df['trans_time'] = df.apply(lambda x: pd.datetime.combine(x['trans_date'], x['trans_time']), 1)

        unusual_dict = {
            "博彩娱乐风险": "博彩娱乐",
            "案件纠纷风险": "案件纠纷",
            "身体健康风险": "医院|保险理赔",
            "不良嗜好风险": "夜间交易",
            "民间借贷风险": "民间借贷",
            "贷款逾期风险": "逾期",
            "投资风险-收购": "收购",
            "投资风险-对外投资": "对外投资",
            "经营性负债": "预收款",
            "经营性风险": "分红退股",
            "典当风险": "典当",
            "公安交易风险": "公安",
            "家庭稳定风险": "家庭不稳定",
            "异常金额-大额整进整出": "整进整出",
            "异常金额-偶发大额": "偶发大额",
            "异常金额-大额快进快出": "快进快出",
            "股票投机风险": "股票投机",
            "变现风险": "变现",
            "存在理财": "理财",
            "担保异常": "担保异常",
            "存在代偿": "代偿"
        }

        json = ""
        for risk in unusual_dict:
            temp_df = df[df['unusual_trans_type'].str.contains( unusual_dict[risk] )].drop(columns= ['unusual_trans_type',
                                                                                                     'trans_date'])
            json +=  f"\"{risk}\":" + temp_df.to_json(orient='records').encode('utf-8').decode("unicode_escape") + ","

        self.variables["异常交易风险"] = "{" + json[:-1] + "}"
