
from abc import ABC

from mapping.module_processor import ModuleProcessor
import datetime
import pandas as pd


class TransModuleProcessor(ModuleProcessor, ABC):

    def __init__(self):
        super().__init__()


    def _time_interval(self, tablename, year=1):
        flow_df = self.cached_data(tablename)
        flow_df['trans_date'] = pd.to_datetime(flow_df['trans_date'])
        max_date = max(flow_df['trans_date'])
        min_date = min(flow_df['trans_date'])

        years_before_first = datetime.datetime(max_date.year - year, max_date.month, 1)
        min_date = min(min_date, years_before_first)
        flow_df = flow_df[(flow_df.trans_date >= min_date) &
                          (flow_df.trans_date <= max_date)]
        return flow_df