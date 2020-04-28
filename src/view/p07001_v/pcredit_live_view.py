from mapping.module_processor import ModuleProcessor


class PcreditLiveView(ModuleProcessor):

    def process(self):
        self._get_live_msg()

    def _get_live_msg(self):
        df=self.cached_data.get("pcredit_live")
        if df is None or df.empty:
            return
        self.variables["live_address"]=df[df['no']==1].loc[:,'live_address'].tolist()[0]
        self.variables["live_address_type"]=df[df['live_address_type'].isin(['01','02','06','11'])].shape[0]