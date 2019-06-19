from mapping.mysql_reader import sql_to_df


class PublicSecurity:
    """
    公安相关的变量模块
    """

    def __init__(self, user_name, id_card_no) -> None:
        super().__init__()
        self.user_name = user_name
        self.id_card_no = id_card_no
        self.load_data_sql = """
            SELECT user_name, id_card_no, result FROM info_certification 
            WHERE certification_type = 'ID_NAME' AND unix_timestamp(NOW()) < unix_timestamp(expired_at)
            AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s
            ORDER BY expired_at DESC LIMIT 1;
        """

    def ps_name_id(self):
        """
        判断用户名和证件号是否匹配
        :param user_name:
        :param id_card_no:
        :return: 如果匹配返回 1， 不匹配返回 0
        """
        df = sql_to_df(sql=(self.load_data_sql), params={"user_name": self.user_name, "id_card_no": self.id_card_no})
        result = 0
        if len(df) == 1 and df['result'][0] == b'\x01':
            result = 1
        return result
