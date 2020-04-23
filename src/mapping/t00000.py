import datetime

from mapping.tranformer import Transformer
from util.id_card_info import GetInformation
from util.mysql_reader import sql_to_df

# 婚姻状态取值如下：
# 未婚 UNMARRIED
# 已婚 MARRIED
# 初婚 CHUHUN
# 再婚 ZAIHUN
# 复婚 FUHUN
# 离异 DIVORCE
# 丧偶 WIDOWHOOD
# 未说明 UNKNOWN
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
            'base_type': 'PERSON',
            'base_phone': '',
            'product_code': '',
            'base_marry_state': 'UNKNOWN'
        }

    def _base_black(self):
        sql = """
        SELECT count(1) as "base_black" FROM info_black_list
            WHERE valid > 0 AND user_name = %(user_name)s AND id_card_no = %(id_card_no)s;
        """
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        if df is not None and len(df) > 0:
            if df['base_black'][0] > 0:
                self.variables['base_black'] = 1

    def transform(self):
        if hasattr(self, "product_code"):
            self.variables["product_code"] = self.product_code
        self.variables['base_idno'] = self.id_card_no
        self.variables['base_phone'] = self.phone
        if self.base_type is not None:
            self.variables['base_type'] = self.base_type
        else:
            self.variables['base_type'] = self.user_type
        self._base_black()
        if self.user_type == 'PERSONAL' and self.id_card_no is not None:
            information = GetInformation(self.id_card_no)
            self.variables['base_gender'] = information.get_sex()
            self.variables['base_age'] = information.get_age()

        if self.origin_data:
            apply_amount = self.origin_data.get("applyAmo")
            if apply_amount:
                self.variables["base_apply_amo"] = apply_amount
            marry_state = self.origin_data.get("marryState")
            if marry_state:
                self.variables["base_marry_state"] = marry_state
