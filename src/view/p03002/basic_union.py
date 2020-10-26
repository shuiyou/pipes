import pandas as pd
from file_utils.files import file_content

from mapping.grouped_tranformer import GroupedTransformer, invoke_union
from util.common_util import get_query_data
from util.mysql_reader import sql_to_df


def _info_com_bus_shareholder(user_name, id_card_no):
    sql = '''
           SELECT a.ent_name,b.share_holder_name,b.share_holder_type,b.sub_conam,b.funded_ratio,b.con_date,b.con_form,c.ratio,c.quantity
            FROM info_com_bus_basic a LEFT JOIN info_com_bus_shareholder b on a.id = b.basic_id
            LEFT JOIN info_com_bus_top c on a.id = b.basic_id
            where a.ent_name = %(user_name)s and a.credit_code = %(id_card_no)s 
            and a.channel_api_no='24001' 
            AND unix_timestamp(NOW()) < unix_timestamp(a.expired_at) 
            order by a.id desc limit 1
       '''
    # if pd.notna(id_card_no):
    #     sql += ' and a.credit_code = %(id_card_no)s'
    # sql += ''' AND unix_timestamp(NOW()) < unix_timestamp(expired_at) and channel_api_no='24001' order by id desc
    #       limit 1 '''
    df = sql_to_df(sql=sql,
                   params={"user_name": user_name,
                           "id_card_no": id_card_no})
    return df


class BasicUnion(GroupedTransformer):

    def invoke_style(self) -> int:
        return invoke_union

    def group_name(self):
        return "basic"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'basic_share_ent_name': [],
            'basic_share_holder_name': [],
            'basic_share_holder_type': [],
            'basic_share_sub_conam': [],
            'basic_share_ratio': [],
            'basic_share_quantity': [],
            'basic_share_funded_ratio': [],
            'basic_share_con_date': [],
            'basic_share_con_form': []
        }

    def clean_variables_shareholder(self):
        resp = get_query_data(self.full_msg, 'COMPANY', '01')
        df = None
        for i in resp:
            user_name = i.get("name")
            id_card_no = i.get("id_card_no")
            df_shareholder = _info_com_bus_shareholder(user_name, id_card_no)
            if not df_shareholder.empty and df is not None:
                df = pd.concat(df, df_shareholder)
            if df is None and not df_shareholder.empty:
                df = df_shareholder
        if df is None:
            return
        df = df.sort_values(by='funded_ratio', ascending=False)
        self.variables['basic_share_ent_name'] = df['ent_name'].to_list()
        self.variables['basic_share_holder_name'] = df['share_holder_name'].to_list()
        self.variables['basic_share_holder_type'] = df['share_holder_type'].to_list()
        self.variables['basic_share_sub_conam'] = df['sub_conam'].to_list()
        self.variables['basic_share_ratio'] = df['ratio'].to_list()
        self.variables['basic_share_quantity'] = df['quantity'].to_list()
        self.variables['basic_share_funded_ratio'] = df['funded_ratio'].to_list()
        self.variables['basic_share_con_date'] = df['con_date'].to_list()
        self.variables['basic_share_con_form'] = df['con_form'].to_list()

    def transform(self):
        self.clean_variables_shareholder()
