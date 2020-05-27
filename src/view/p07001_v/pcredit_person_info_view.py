from mapping.module_processor import ModuleProcessor


# 个人基本信息
class PcreditPersonInfoView(ModuleProcessor):

    def process(self):
        self._get_person_info_basic_msg()

    def _get_person_info_basic_msg(self):
        person_info_df = self.cached_data.get("pcredit_person_info")
        if person_info_df is None or person_info_df.empty:
            return
        # 性别
        self.variables["sex"] = person_info_df.loc[0, 'sex']
        # 婚姻状态
        self.variables["info_marriage_status"] = person_info_df.loc[0, 'marriage_status']
        # 就业状态
        self.variables["employment"] = person_info_df.loc[0, 'employment']
        # 通讯地址
        self.variables["communication_address"] = person_info_df.loc[0, 'communication_address']
        # 户籍地址
        self.variables["residence_address"] = person_info_df.loc[0, 'residence_address']
        # spouse_name
        self.variables["spouse_name"] = person_info_df.loc[0, 'spouse_name']
        # 配偶证件号
        self.variables["spouse_certificate_no"] = person_info_df.loc[0, 'spouse_certificate_no']
