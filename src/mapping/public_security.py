from mapping.mysql_reader import sql_to_df


class PublicSecurity:
    """
    公安相关的变量模块
    """

    def __init__(self, user_name, id_card_no) -> None:
        super().__init__()
        self.user_name = user_name
        self.id_card_no = id_card_no

        self.ps_dict = {
            'ps_name_id': 0,
            'ps_run': 0,
            'ps_drug': 0,
            'ps_involve_drug': 0,
            'ps_seri_crim': 0,
            'ps_mali_crim': 0,
            'ps_econ_crim': 0,
            'ps_amend_staff': 0,
            'ps_ileg_crim': 0
        }

    def info_certification_df(self):
        info_certification = """
            SELECT user_name, id_card_no, result FROM info_certification 
            WHERE certification_type = 'ID_NAME' AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s
            ORDER BY expired_at DESC LIMIT 1;
        """
        df = sql_to_df(sql=(info_certification),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def ps_name_id(self, df=info_certification_df):
        """
        判断用户名和证件号是否匹配
        :param user_name:
        :param id_card_no:
        :return: 如果匹配返回 1， 不匹配返回 0
        """
        result = 0
        if len(df) == 1 and df['result'][0] == b'\x01':
            result = 1
        self.ps_dict['ps_name_id'] = result

    def crime_type_df(self):
        info_criminal_case = """
            SELECT user_name, id_card_no, crime_type FROM info_criminal_case 
            WHERE certification_type = 'ID_NAME' AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s
            ORDER BY expired_at DESC LIMIT 1;
        """
        df = sql_to_df(sql=(info_criminal_case),
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def ps_crime_type(self, df=crime_type_df):
        crime_type = df['crime_type'].split(',')
        if 'AT_LARGE' in crime_type:
            self.ps_dict['ps_run'] = 1
        if 'DRUG' in crime_type:
            self.ps_dict['ps_drug'] = 1
        if "DRUG_RELATED" in crime_type or "ILLEGAL_F" in crime_type:
            self.ps_dict['ps_involve_drug'] = 1
        if 'ILLEGAL_B' in crime_type:
            self.ps_dict['ps_seri_crim'] = 1
        if 'ILLEGAL_C' in crime_type:
            self.ps_dict['ps_mali_crim'] = 1
        if 'ILLEGAL_E' in crime_type:
            self.ps_dict['ps_econ_crim'] = 1
        if 'REVOKE' in crime_type:
            self.ps_dict['ps_amend_staff'] = 1
        if 'ILLEGAL_A' in crime_type:
            self.ps_dict['ps_illeg_crim'] = 1
