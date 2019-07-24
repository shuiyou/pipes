import datetime

from util.id_card_info import GetInformation
from util.mysql_reader import sql_to_df
from mapping.tranformer import Transformer


class T00000(Transformer):
    """
    基本信息
    """

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'base_date': datetime.datetime.now().strftime('%Y-%m-%d'),
            'base_idno': '',
            'base_gender': 0,
            'base_age': 0,
            'base_black': 0,
            'base_type': 'PERSON'
        }

    def _base_black(self):
        sql = """
        SELECT count(1) as "base_black" FROM info_black_list
            WHERE user_name = %(user_name)s AND id_card_no = %(id_card_no)s;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        if df is not None and len(df) > 0:
            if df['base_black'][0] > 0:
                self.variables['base_black'] = 1

    def transform(self):
        self.variables['base_idno'] = self.id_card_no
        self.variables['base_type'] = self.user_type
        self._base_black()
        if self.user_type == 'PERSONAL' and self.id_card_no is not None:
            information = GetInformation(self.id_card_no)
            self.variables['base_gender'] = information.get_sex()
            self.variables['base_age'] = information.get_age()
            self.variables['base_division'] = information.get_division()
