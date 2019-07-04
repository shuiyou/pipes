import datetime

import pandas as pd
from faker import Faker

from mapping.mapper import translate
from mapping.mysql_reader import sql_insert
from mapping.mysql_reader import sql_to_df
from data.process_excel_case import Process


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

# 处理第一张主表数据
def _insert_main_table_data(title, key, channel_api_no, expired_at='2030-12-20'):
    title_array = title.split(';')
    key_array = key.split(';')
    fake = Faker(locale='zh_CN')
    # 用faker生成查询条件对应的字段
    name_key_word = ''
    name_key_value = ''
    id_no_key_word = ''
    id_no_key_value = ''
    phone_key_word = ''
    phone_key_value = ''
    for key_value in key_array:
        key_value = key_value.replace('\n', '').replace('\r', '')
        if key_value in ['unique_name', 'user_name', 'name']:
            name_key_word = key_value
            name_key_value = fake.name()
        if key_value in ['unique_id_no', 'id_card_no']:
            id_no_key_word = key_value
            id_no_key_value = fake.ssn()
        if ('phone' == key_value):
            phone_key_word = key_value
            phone_key_value = fake.phone_number()
    create_time = '\'' + datetime.datetime.now().strftime("%Y-%m-%d") + '\''
    # 表名
    table_name = title_array[0].split('.')[0]
    # 关联子表的主键
    key_word_sub_table = title_array[0].split('.')[1]
    # 拼接sql
    main_table_sql = """
    insert into 
    """
    main_table_sql += ' ' + table_name + ' ('
    # 关联主键不是id，faker一个数字
    if len(key_word_sub_table) > 0 and 'id' != key_word_sub_table:
        main_table_sql += key_word_sub_table + ','
    if len(name_key_word) > 0:
        main_table_sql += name_key_word + ','
    if len(id_no_key_word) > 0:
        main_table_sql += id_no_key_word + ','
    if len(phone_key_word) > 0:
        main_table_sql += phone_key_word + ','
    # 主表要插入字段
    if (len(title_array) > 1):
        count = 0
        for info in title_array:
            count = count + 1
            if count > 1:
                main_table_sql += info.split('=')[0].split('.')[1] + ','
    main_table_sql += 'expired_at,channel_api_no) values ('
    if len(key_word_sub_table) > 0 and 'id' != key_word_sub_table:
        main_table_sql += '\'' + str(fake.random_int()) + str(fake.random_int()) + '\'' + ','
    if len(name_key_value) > 0:
        main_table_sql += '\'' + name_key_value + '\'' + ','
    if len(id_no_key_value) > 0:
        main_table_sql += '\'' + id_no_key_value + '\'' + ','
    if len(phone_key_value) > 0:
        main_table_sql += phone_key_value + ','
    if (len(title_array) > 1):
        count = 0
        for info in title_array:
            count = count + 1
            if count > 1:
                main_table_sql += info.split('=')[1] + ','
    main_table_sql += '\'' + expired_at + '\'' + ','
    main_table_sql += '\'' + channel_api_no.split('.')[0] + '\'' + ')'
    print('insert-sql--' + main_table_sql)
    sql_insert(sql=main_table_sql)

    # 插入成功后查询出主键
    info_main_table_sql = """
    select 
    """
    if len(key_word_sub_table) > 0:
        info_main_table_sql += key_word_sub_table
    if len(name_key_word) > 0:
        info_main_table_sql += ',' + name_key_word
    if len(id_no_key_word) > 0:
        info_main_table_sql += ',' + id_no_key_word
    if len(phone_key_word) > 0:
        info_main_table_sql += ',' + phone_key_word
    info_main_table_sql += ' from ' + table_name + ' where '
    if len(name_key_word) > 0:
        info_main_table_sql += name_key_word + '=' + '\'' + name_key_value + '\'' + ' and '
    if len(id_no_key_word) > 0:
        info_main_table_sql += id_no_key_word + '=' + '\'' + id_no_key_value + '\'' + ' and '
    if len(phone_key_word) > 0:
        info_main_table_sql += phone_key_word + '=' + '\'' + phone_key_value + '\'' + ' and '
    info_main_table_sql = info_main_table_sql[0:len(info_main_table_sql) - 4]
    print('query-sql--' + info_main_table_sql)
    df = sql_to_df(sql=info_main_table_sql)
    key = '{'
    if len(name_key_word) > 0:
        key += '\"' + name_key_word + '\"' + ':'
        key += '\"' + df[name_key_word][0] + '\"' + ','
    if len(id_no_key_word) > 0:
        key += '\"' + id_no_key_word + '\"' + ':'
        key += '\"' + df[id_no_key_word][0] + '\"' + ','
    if len(phone_key_word) > 0:
        key += '\"' + phone_key_word + '\"' + ':'
        key += '\"' + df[phone_key_word][0] + '\"' + ','
    key = key[0:len(key) - 1]
    key += '}'
    df['key'] = key
    df['key_sub'] = df[key_word_sub_table]
    return df


# 处理第一张主表数据对应的子表数据
def _insert_main_table_sub_data(title, df_main_id):
    title = title.replace('\n', '').replace('\r', '')
    value_array = title.split(';')
    table_name = ''
    relation_mian_table_key = ''
    # 字段种类
    field_count = 0
    field_array = []
    # 描述有多少组数据
    data_group_count = 0
    sql = """
         insert into 
         """
    key_value_array = []
    if len(value_array) > 0:
        table_name = value_array[0].split('.')[0]
        relation_mian_table_key = value_array[0].split('.')[1]
        for detail in value_array:
            if detail.find('[0]') >= 0:
                field_count = field_count + 1
                field_array.append(detail.split('[0]')[0].split('.')[1])
        if field_count > 0:
            data_group_count = (len(value_array) - 1) / field_count
            data_group_count = int(data_group_count)

        if data_group_count > 0:
            for i in range(data_group_count):
                data = []
                for field in field_array:
                    for detail in value_array:
                        if detail.find(field + '[' + str(i) + ']') >= 0:
                            data.append(detail.split('=')[1])
                key_value_array.append(data)
    sql += ' ' + table_name + ' ('
    if len(field_array) > 0:
        sql += ','.join(field_array) + ','
    sql += relation_mian_table_key
    sql += ') values '
    if len(key_value_array) > 0:
        for data in key_value_array:
            sql += '(' + ','.join(data) + ',' + str(df_main_id) + '),'
    else:
        sql += '(' + str(df_main_id) + '),'
    sql = sql[0:len(sql) - 1]
    print(sql)
    sql_insert(sql=sql)


class deposit(Process):

    def read_excel_as_df(self):
        path = self.read_path
        df = pd.read_excel(path)
        return df

    def insert_data(self, df=None):
        if df is not None and len(df) > 0:
            # 去掉用例标题空行
            no_empty_df = df.dropna(subset=['用例标题'], how='any')
            # 获取标题的list集合
            title_list = no_empty_df.columns.values.tolist()
            # 遍历no_empty_df逐条插入数据
            key_value = []
            for index, row in no_empty_df.iterrows():
                df_main_id = 0
                for title in title_list:
                    if 'table_main' == title:
                        # 插入第一张主表的数据，返回多条数据的主表子表关联主键
                        title = str(row[title])
                        key = str(row['测试用例key'])
                        channel_api_no = str(row['测试模块'])
                        if (len(channel_api_no)) < 5:
                            channel_api_no = '0' + channel_api_no
                        df_main = _insert_main_table_data(title, key, channel_api_no)
                        df_main_id = df_main['key_sub'][0]
                        key_value.append(df_main['key'][0])
                    if title.find('table_main_sub') >= 0:
                        # 插入第一张主表关联的子表数据
                        title = str(row[title])
                        if title is not None and title != 'nan' and len(title) > 0:
                            _insert_main_table_sub_data(title, df_main_id)
            no_empty_df['keyValue'] = key_value
            return no_empty_df

    def write_df_into_excel(self,df=None):
        path = self.write_path
        if df is not None and len(df) > 0:
            # path += str(df['测试模块'][0]).split('.')[0] + '-' + '测试用例结果.xlsx'
            df.to_excel(path)
        return path

    def run_processor(self, path):
        df = pd.read_excel(path)
        # 跑代码的实际结果
        actual_reslut_array = []
        # 与预期结果对比是否通过
        is_pass_array = []
        for index, row in df.iterrows():
            code_array = []
            code = str(row['测试模块'])
            if (len(code)) < 5:
                code = '0' + code
            code_array.append(code)
            field = row['用例标题']
            expect_result = row['预期测试结果'].split('=')[1]
            params = eval(row['keyValue'])
            user_name = ''
            id_card_no = ''
            phone = ''
            # 运行代码生成的用例字段结果
            value = ''
            for key, value in params.items():
                if key in ['user_name', 'unique_name', 'name']:
                    user_name = value
                if key in ['id_card_no', 'unique_id_no']:
                    id_card_no = value
                if key in ['phone']:
                    phone = value
            res = translate(code_array, user_name=user_name, id_card_no=id_card_no, phone=phone)
            print('测试用例' + code + '返回结果----')
            print(res)
            for key in res:
                if field == key:
                    value = res[key]
                    actual_reslut_array.append(value)
            if is_number(expect_result):
                if float(value) == float(expect_result):
                    is_pass_array.append("true")
                else:
                    is_pass_array.append("false")
            else:
                if str(value) == str(expect_result):
                    is_pass_array.append("true")
                else:
                    is_pass_array.append("false")
        df['实际测试结果'] = actual_reslut_array
        df['是否通过'] = is_pass_array
        df.to_excel(path)

    def do_process_case(self, read_path=None, write_path=None):
        df = self.read_excel_as_df()
        df_write_excel = self.insert_data(df=df)
        path = self.write_df_into_excel(df=df_write_excel)
        self.run_processor(path=path)
