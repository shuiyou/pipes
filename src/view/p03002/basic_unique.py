import datetime

import pandas as pd

from mapping.grouped_tranformer import GroupedTransformer
from util.mysql_reader import sql_to_df


class BasicUnique(GroupedTransformer):

    def invoke_style(self) -> int:
        return self.invoke_each

    def group_name(self):
        return "basic"

    def __init__(self) -> None:
        super().__init__()
        self.variables = {
            'basic_ex_ent_name': "",
            'basic_ex_industry_phyname': "",
            'basic_ex_ent_status': "",
            'basic_ex_reg_cap': "",
            'basic_ex_open_date_range': "",
            'basic_ent_name': "",
            'basic_fr_name': "",
            'basic_es_date': "",
            'basic_appr_date': "",
            'basic_industry_phyname': "",
            'basic_address': "",
            'basic_operate_scope': "",
            'basic_ent_type': "",
            'basic_credit_code': "",
            'basic_reg_cap': "",
            'basic_ent_status': "",
            'basic_open_date_range': "",
            'basic_share_ent_name': ""
        }

    def _info_com_bus_basic(self):
        sql='''
            SELECT id FROM info_com_bus_basic WHERE ent_name=%(user_name)s
        '''
        if pd.notna(self.id_card_no):
            sql += ' AND credit_code = %(id_card_no)'
        sql += '''AND unix_timestamp(NOW()) < unix_timestamp(expired_at) and channel_api_no='24001' order by id desc 
        limit 1 '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        return df

    def _info_com_bus_face(self, id):
        sql = '''
            select * from info_com_bus_face where basic_id = %(id)s
        '''
        df = sql_to_df(sql=sql,
                       params={"user_name": self.user_name,
                               "id_card_no": self.id_card_no})
        return df

    def clean_variables_face(self, basic_df):
        id = basic_df.loc[0, 'id']
        face_df = self._info_com_bus_face(id)
        if face_df.empty:
            return
        self.variables['basic_ex_ent_name'] = self.user_name
        self.variables['basic_ex_industry_phyname'] = face_df.loc[0, 'industry_phyname']
        self.variables['basic_ex_ent_status'] = face_df.loc[0, 'ent_status']
        self.variables['basic_ex_reg_cap'] = face_df.loc[0, 'reg_cap']
        open_from = "" if pd.isna(face_df.loc[0, 'open_from']) else datetime.datetime.strftime(face_df.loc[0, 'open_from'], "%Y-%m-%d, %H:%M:%S")
        open_to = "" if pd.isna(face_df.loc[0, 'open_to']) else datetime.datetime.strftime(face_df.loc[0, 'open_to'], "%Y-%m-%d, %H:%M:%S")
        self.variables['basic_ex_open_date_range'] = open_from + "-" + open_to
        if self.origin_data.get("extraParam").get("strategy") == '02':
            return
        self.variables['basic_ent_name'] = self.user_name
        self.variables['basic_fr_name'] = face_df.loc[0, 'fr_name']
        self.variables['basic_es_date'] = "" if pd.isna(face_df.loc[0, 'es_date']) else datetime.datetime.strftime(face_df.loc[0, 'es_date'], "%Y-%m-%d, %H:%M:%S")
        self.variables['basic_appr_date'] = "" if pd.isna(face_df.loc[0, 'appr_date']) else datetime.datetime.strftime(face_df.loc[0, 'appr_date'], "%Y-%m-%d, %H:%M:%S")
        self.variables['basic_industry_phyname'] = face_df.loc[0, 'industry_phyname']
        self.variables['basic_address'] = face_df.loc[0, 'address']
        self.variables['basic_operate_scope'] = face_df.loc[0, 'operate_scope']
        self.variables['basic_ent_type'] = face_df.loc[0, 'ent_type']
        self.variables['basic_credit_code'] = self.id_card_no
        self.variables['basic_reg_cap'] = face_df.loc[0, 'reg_cap']
        self.variables['basic_ent_status'] = face_df.loc[0, 'ent_status']
        self.variables['basic_open_date_range'] = open_from + "-" + open_to
        self.variables['basic_share_ent_name'] = face_df.loc[0, 'ent_status']

    def transform(self):
        basic_df = self._info_com_bus_basic()
        if basic_df.empty:
            return
        self.clean_variables_face(basic_df)