from mapping.module_processor import ModuleProcessor


class PcreditBizInfoView(ModuleProcessor):

    def process(self):
        pass

    def _get_biz_info_msg(self):
        biz_info_df=self.cached_data.get["pcredit_biz_info"]
        if biz_info_df is None or biz_info_df.empty:
            return
        biz_type_df=biz_info_df[biz_info_df['biz_type']=="01"]
        if biz_type_df is None or biz_type_df.empty:
            return
        biz_type_df=biz_type_df.sort_values(by='biz_first_month')
        self.variables["biz_firstmonth"]=biz_type_df.loc[:,'biz_firstmonth'].tolist()[0]