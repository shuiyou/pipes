from faker import Faker

from data.process_excel_case import Process
from mapping.mysql_reader import sql_insert
from mapping.mysql_reader import sql_to_df


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


# 处理第二张主表数据
def _insert_main_1_table_data(title, key, channel_api_no, expired_at='2030-12-20'):
    title.replace('\n', '').replace('\r', '')
    title_array = title.split(';')
    key_array = key.split(';')
    # 查询条件-多组
    query_key_value_array = []
    fake = Faker(locale='zh_CN')
    # 表名
    table_name = title_array[0].split('.')[0]
    # 关联子表的主键
    key_word_sub_table = title_array[0].split('.')[1]
    # 封装插入主表的字段
    insert_key_array = []
    # 封装插入主表的数据-多组
    insert_value_array = []
    # 描述有多少组数据
    data_group_count = 0
    # 每组字段种类
    field_count = 0
    field_array = []

    if len(key_word_sub_table) > 0 and 'id' != key_word_sub_table:
        insert_key_array.append(key_word_sub_table)
        insert_value_array.append(str(fake.random_int()) + str(fake.random_int()))
        # 主表要插入字段
    if (len(title_array) > 0):
        for info in title_array:
            key_value_array = []
            if info.find('[0]') >= 0:
                field_count = field_count + 1
                insert_key_array.append(info.split('[0]')[0].split('.')[1])
                field_array.append(info.split('[0]')[0].split('.')[1])
            for key in key_array:
                main_table_key = info.split('=')[0].split('.')[1]
                main_table_key = main_table_key[0:len(main_table_key) - 3]
                if key == main_table_key:
                    key_value_array.append(main_table_key)
                    key_value_array.append(info.split('=')[1])
                    query_key_value_array.append(key_value_array)
        if field_count > 0:
            data_group_count = (len(title_array) - 1) / field_count
            data_group_count = int(data_group_count)
        if data_group_count > 0:
            for i in range(data_group_count):
                data = []
                for detail in title_array:
                    if detail.find('[' + str(i) + ']') >= 0:
                        data.append(detail.split('=')[1])
                data.append('\'' + expired_at + '\'')
                data.append('\'' + channel_api_no.split('.')[0] + '\'')
                insert_value_array.append(data)
    insert_key_array.append('expired_at')
    insert_key_array.append('channel_api_no')
    # 拼接sql
    main_table_sql = """
        insert into 
        """
    main_table_sql += ' ' + table_name + ' ('
    main_table_sql += ','.join(insert_key_array)
    main_table_sql += ') values '
    for data in insert_value_array:
        main_table_sql += '(' + ','.join(data) + '),'
    main_table_sql = main_table_sql[0:len(main_table_sql) - 1]
    print('insert-sql--' + main_table_sql)
    sql_insert(sql=main_table_sql)

    # 插入成功后查询出主键
    key_word_sub_table_array = []
    for key_value_array in query_key_value_array:
        info_main_table_sql = """
            select 
            """
        if len(key_word_sub_table) > 0:
            info_main_table_sql += key_word_sub_table
        info_main_table_sql += ' from ' + table_name + ' where '
        info_main_table_sql += str(key_value_array[0]) + '=' + str(key_value_array[1]) + ' and '
        info_main_table_sql = info_main_table_sql[0:len(info_main_table_sql) - 4]
        print('query-sql' + info_main_table_sql)
        df = sql_to_df(sql=info_main_table_sql)
        # df['key_sub'] = df[key_word_sub_table]
        key_word_sub_table_array.append(df[key_word_sub_table][0])
    return key_word_sub_table_array


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


# 处理第二张主表数据对应子表数据
def _insert_main_1_table_sub_data(title, df_main_id_array):
    title = title.replace('\n', '').replace('\r', '')
    value_array = title.split(';')
    table_name = ''
    relation_mian_table_key = ''
    # 字段种类
    field_count = 0
    field_array = []
    insert_key_array = []
    if len(value_array) > 0:
        table_name = value_array[0].split('.')[0]
        relation_mian_table_key = value_array[0].split('.')[1]
        for detail in value_array:
            if detail.find('[0-0]') >= 0:
                field_count = field_count + 1
                field_array.append(detail.split('[0-0]')[0].split('.')[1])
                insert_key_array.append(detail.split('[0-0]')[0].split('.')[1])
        insert_key_array.append(relation_mian_table_key)
        for i in range(len(df_main_id_array)):
            insert_value_array = []
            for j in range(5):
                data = []
                number = 0
                for detail in value_array:
                    if detail.find('[' + str(i) + '-' + str(j) + ']') >= 0:
                        insert_value = detail.split('=')[1]
                        number = 1
                if number > 0:
                    data.append(str(df_main_id_array[i]))
                    insert_value_array.append(data)
            if len(insert_value_array) > 0:
                sql = """
                         insert into 
                         """
                sql += ' ' + table_name + ' ('
                sql += ','.join(insert_key_array)
                sql += ') values '
                for data in insert_value_array:
                    sql += '(' + ','.join(data) + '),'
                sql = sql[0:len(sql) - 1]
                print(sql)
                sql_insert(sql=sql)


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
                            insert_value = detail.split('=')[1]
                            data.append(insert_value)
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


class unit_multi_deposit(Process):

    def read_excel_as_df(self):
        path = self.read_path
        return super().read_excel_as_df(path)

    def write_df_into_excel(self, df=None):
        path = self.write_path
        return super().write_df_into_excel(path, df=df)

    def run_processor(self, path):
        return super().run_processor(path=path)

    def insert_data(self, df=None):
        if df is not None and len(df) > 0:
            # 去掉用例标题空行
            no_empty_df = df.dropna(subset=['用例标题'], how='any')
            # 获取标题的list集合
            title_list = no_empty_df.columns.values.tolist()
            # 遍历no_empty_df逐条插入数据
            key_value_main_1 = []
            for index, row in no_empty_df.iterrows():
                df_main_id = 0
                df_main_1_key_array = []
                for title in title_list:
                    if 'table_main' == title:
                        # 插入第一张主表的数据，返回多条数据的主表子表关联主键
                        title = str(row[title])
                        key = str(row['main_key'])
                        channel_api_no = str(row['测试模块'])
                        df_main = _insert_main_table_data(title, key, channel_api_no)
                        df_main_id = df_main['key_sub'][0]
                        key_value_main_1.append(df_main['key'][0])
                    if title.find('table_main_sub') >= 0:
                        # 插入第一张主表关联的子表数据
                        title = str(row[title])
                        if title is not None and title != 'nan' and len(title) > 0:
                            _insert_main_table_sub_data(title, df_main_id)
                    if 'table_main_1' == title:
                        # 插入第二张主表的数据，返回多条数据的主表子表关联主键
                        title = str(row[title])
                        key = str(row['main_1_key'])
                        channel_api_no = str(row['测试模块'])
                        if (len(channel_api_no)) < 5:
                            channel_api_no = '0' + channel_api_no
                        df_main_1_key_array = _insert_main_1_table_data(title, key, channel_api_no)
                    if title.find('table_main_1_sub') >= 0:
                        # 插入第二张主表关联的子表数据
                        title = str(row[title])
                        if title is not None and title != 'nan' and len(title) > 0:
                            _insert_main_1_table_sub_data(title, df_main_1_key_array)
            no_empty_df['key_value_main'] = key_value_main_1
            return no_empty_df

    def do_process_case(self, read_path=None, write_path=None):
        df = self.read_excel_as_df()
        df_write_excel = self.insert_data(df=df)
        path = self.write_df_into_excel(df=df_write_excel)
        return self.run_processor(path=path)
