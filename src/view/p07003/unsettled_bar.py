from mapping.grouped_tranformer import GroupedTransformer, invoke_each
import pandas as pd
from datetime import datetime,timedelta

class UnsettledBar(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_each

    def group_name(self):
        return "unsettled_bar"

    def __init__(self, df = None) -> None:
        super().__init__()
        self.variables = {
            "guar_type": [],
            "guar_amt": [],
            "guar_bal": [],
            "guar_cnt": [],
            "bus_bar": [],
            "discount_total_recent":0,
            "grant_history_bar": []
        }
        self.df = df

    def transform(self):

        data = self.df
        self.variables["guar_type"] = ['信用/免担保','保证','质押','抵押','组合']
        guar_amt = []
        guar_bal = []
        guar_cnt = []
        for guar in self.variables["guar_type"]:
            guar_amt.append(
                data[data.guar_type == guar].guar_amt.sum()
            )
            guar_bal.append(
                data[data.guar_type == guar].cur_bal.sum()
            )
            guar_cnt.append(
                data[data.guar_type == guar].shape[0]
            )
        self.variables["guar_amt"] = guar_amt
        self.variables["guar_bal"] = guar_bal
        self.variables["guar_cnt"] = guar_cnt

        biz_list = set(data.bus_type.tolist())
        bus_bar = []
        for biz in biz_list:
            temp_dict = {}
            temp_df = data[data.bus_type == biz]
            temp_dict['bus_type'] = biz
            temp_dict['bus_amt'] = temp_df.grant_amt.sum()
            temp_dict['bus_bal'] = temp_df.cur_bal.sum()
            temp_dict['bus_cnt'] = temp_df.shape[0]
            temp_dict['bus_bal_detail'] = temp_df.groupby('bus_sup')['cur_bal'].agg({'sup_amt':'sum'}).reset_index().to_dict(orient='list')
            bus_bar.append(temp_dict)
        self.variables["bus_bar"]  = bus_bar

        self.variables["discount_total_recent"] = data[(data.bus_sup.str.contains("贴现"))
                                &(pd.to_datetime(data.start_date) > datetime.now() - timedelta(days = 365))].cur_bal.sum()

        report_year  = pd.to_datetime(self.variables["report_time"]).year
        year_list = [report_year-3,report_year-2,report_year-1,report_year]

        grant_history_bar = []
        for inst in set(data.inst_name.tolist()):

            temp_dict = {}
            temp_dict["inst_name"] = inst
            temp_dict["year_one"] = {
                "year" : year_list[0],
                "grant_total" : data[pd.to_datetime(data.start_date).year == year_list[0] ].grant_amt.sum(),
                "bus_detail" : data[pd.to_datetime(data.start_date).year == year_list[0] ]\
                    .groupby('bus_type')['grant_amt'].agg({'bus_grant_total':'sum'}).reset_index().to_dict(orient='list')
            }
            temp_dict["year_two"] = {
                "year": year_list[1],
                "grant_total": data[pd.to_datetime(data.start_date).year == year_list[1]].grant_amt.sum(),
                "bus_detail": data[pd.to_datetime(data.start_date).year == year_list[1]] \
                    .groupby('bus_type')['grant_amt'].agg({'bus_grant_total': 'sum'}).reset_index().to_dict(
                    orient='list')
            }
            temp_dict["year_three"] = {
                "year": year_list[2],
                "grant_total": data[pd.to_datetime(data.start_date).year == year_list[2]].grant_amt.sum(),
                "bus_detail": data[pd.to_datetime(data.start_date).year == year_list[2]] \
                    .groupby('bus_type')['grant_amt'].agg({'bus_grant_total': 'sum'}).reset_index().to_dict(
                    orient='list')
            }
            temp_dict["year_four"] = {
                "year": year_list[3],
                "grant_total": data[pd.to_datetime(data.start_date).year == year_list[3]].grant_amt.sum(),
                "bus_detail": data[pd.to_datetime(data.start_date).year == year_list[3]] \
                    .groupby('bus_type')['grant_amt'].agg({'bus_grant_total': 'sum'}).reset_index().to_dict(
                    orient='list')
            }
            grant_history_bar.append(temp_dict)

        self.variables["grant_history_bar"] = grant_history_bar