from mapping.mysql_reader import sql_to_df
from mapping.tranformer import Transformer


class T06001(Transformer):
    """
    公安相关的变量模块
    """

    def get_biz_type(self):
        """
        返回这个转换对应的biz type
        :return:
        """
        return ''

    def __init__(self, user_name, id_card_no) -> None:
        super().__init__()
        self.user_name = user_name
        self.id_card_no = id_card_no

        self.variables = {
            'ps_run': 0,
            'ps_drug': 0,
            'ps_involve_drug': 0,
            'ps_seri_crim': 0,
            'ps_mali_crim': 0,
            'ps_econ_crim': 0,
            'ps_amend_staff': 0,
            'ps_ileg_crim': 0
        }

    def _crime_type_df(self):
        info_criminal_case = """
            SELECT crime_type FROM info_criminal_case 
            WHERE certification_type = 'ID_NAME' AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s
            ORDER BY expired_at DESC LIMIT 1;
        """
        df = sql_to_df(sql=info_criminal_case,
                       params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        return df

    def _ps_crime_type(self, df=None):
        if df is not None and 'crime_type' in df.columns and len(df) == 1:
            value = df['crime_type'][0]
            crime_type = [x.strip() for x in value.split(',')] if ',' in value else value
            if 'AT_LARGE' in crime_type:
                self.variables['ps_run'] = 1
            if 'DRUG' in crime_type:
                self.variables['ps_drug'] = 1
            if "DRUG_RELATED" in crime_type or "ILLEGAL_F" in crime_type:
                self.variables['ps_involve_drug'] = 1
            if 'ILLEGAL_B' in crime_type:
                self.variables['ps_seri_crim'] = 1
            if 'ILLEGAL_C' in crime_type:
                self.variables['ps_mali_crim'] = 1
            if 'ILLEGAL_E' in crime_type:
                self.variables['ps_econ_crim'] = 1
            if 'REVOKE' in crime_type:
                self.variables['ps_amend_staff'] = 1
            if 'ILLEGAL_A' in crime_type:
                self.variables['ps_illeg_crim'] = 1

    def transform(self):
        """
        执行变量转换
        :return:
        """
        self._ps_crime_type(self._crime_type_df())

    def variables_result(self):
        """
        返回转换好的结果
        :return: dict对象，包含对应的变量
        """
        return self.variables
