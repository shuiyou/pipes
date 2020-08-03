from mapping.module_processor import ModuleProcessor


# 个人基本信息
class PcreditPersonInfoView(ModuleProcessor):

    def process(self):
        self._get_person_info_basic_msg()

    def _get_person_info_basic_msg(self):
        person_info_df = self.cached_data.get("pcredit_person_info")
        profession_df = self.cached_data.get("pcredit_profession")
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
        # 工作单位
        work_unit_list = profession_df.sort_values(by='no').loc[:, 'work_unit'].to_list()
        work_unit_list = [x for x in work_unit_list if x is not None and x != '--']
        self.variables['work_unit'] = work_unit_list[0] if len(work_unit_list) > 0 else ''
        # 配偶姓名
        self.variables["spouse_name"] = person_info_df.loc[0, 'spouse_name']
        # 配偶证件号
        self.variables["spouse_certificate_no"] = person_info_df.loc[0, 'spouse_certificate_no']
        # 配偶手机号
        self.variables['spouse_phone'] = person_info_df.loc[0, 'spouse_mobile_no']
