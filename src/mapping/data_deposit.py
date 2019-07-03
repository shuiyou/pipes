import pandas as pd
import datetime
from faker import Faker
from mapping.mysql_reader import sql_to_df
from mapping.mysql_reader import sql_insert
from mapping.mapper import translate



#处理第一张主表数据
def _insert_main_table_data(title, key,expired_at = '2030-12-20'):
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
        key_value = key_value.replace('\n','').replace('\r','')
        if ('unique_name' == key_value) or ('uer_name' == key_value):
            name_key_word = key_value
            name_key_value = fake.name()
        if ('unique_id_no' == key_value) or ('id_card_no' == key_value):
            id_no_key_word = key_value
            id_no_key_value = fake.ssn()
        if ('phone' == key_value):
            phone_key_word = key_value
            phone_key_value = fake.phone_number()
    time = '\''+datetime.datetime.now().strftime("%Y-%m-%d")+'\''
    #表名
    table_name = title_array[0].split('.')[0]
    # 拼接sql
    main_table_sql = """
    insert into 
    """
    main_table_sql += ' ' + table_name + ' ('
    if len(name_key_word) > 0:
        main_table_sql += name_key_word
    if len(id_no_key_word) > 0:
        main_table_sql += ',' + id_no_key_word
    if len(phone_key_word) > 0:
        main_table_sql += ',' + phone_key_word
    if title.find('query_date') >= 0 or title.find('create_time') >=0:
        if len(title_array)>=0 and len(title_array[1]) >= 0:
            value_date = title_array[1].replace('\n', '').replace('\r', '')
            if value_date.find('='):
                time = value_date.split('=')[1]
    if title.find('query_date') >= 0:
        main_table_sql += ',query_date'
    main_table_sql += ',create_time,expired_at) values ('
    if len(name_key_value) > 0:
        main_table_sql += '\''+name_key_value+ '\''+ ','
    if len(id_no_key_value) > 0:
        main_table_sql += '\''+id_no_key_value+ '\'' + ','
    if len(phone_key_value) > 0:
        main_table_sql += phone_key_value + ','
    if title.find('query_date') >= 0:
        main_table_sql += time + ','
    main_table_sql += time + ','
    main_table_sql +=  '\''+expired_at+ '\''+ ')'
    sql_insert(sql=main_table_sql)
    #插入成功后查询出主键
    info_main_table_sql = """
    select id
    """
    if len(name_key_word) > 0:
        info_main_table_sql += ','+name_key_word
    if len(id_no_key_word) > 0:
        info_main_table_sql += ',' + id_no_key_word
    if len(phone_key_word) > 0:
        info_main_table_sql += ',' + phone_key_word

    info_main_table_sql += ' from ' + table_name +' where '
    if len(name_key_word) > 0:
        info_main_table_sql += name_key_word + '=' +  '\''+name_key_value+ '\''
    if len(id_no_key_word) > 0:
        info_main_table_sql += ' and ' + id_no_key_word + '=' +  '\''+id_no_key_value+ '\''
    if len(phone_key_word) > 0:
        info_main_table_sql += ' and ' + phone_key_word + '=' +  '\''+phone_key_value+ '\''
    df = sql_to_df(sql=info_main_table_sql)
    key = '{'
    if len(name_key_word) > 0:
        key += '\"'+ name_key_word + '\"'+':'
        key +=  '\"'+ df[name_key_word][0] + '\"'+ ','
    if len(id_no_key_word) > 0:
        key +=  '\"' + id_no_key_word +  '\"' + ':'
        key += '\"'+ df[id_no_key_word][0] + '\"'+ ','
    if len(phone_key_word) > 0:
        key +=  '\"' + phone_key_word +  '\"' + ':'
        key += '\"'+ df[phone_key_word][0] + '\"' + ','
    key = key[0:len(key)-1]
    key += '}'
    df['key'] = key
    return df


#处理第一张主表数据对应的子表数据
def _insert_main_table_sub_data(title,df_main_id, expired_at = '2030-12-20'):
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

        for i in range(data_group_count):
            data = []
            for field in field_array:
                for detail in value_array:
                    if detail.find(field + '[' + str(i) + ']') >= 0:
                        data.append(detail.split('=')[1])
            key_value_array.append(data)
    sql += ' ' + table_name + ' ('
    sql += ','.join(field_array)
    sql += ',' + relation_mian_table_key
    sql += ',expired_at) values '
    for data in key_value_array:
        sql += '(' + ','.join(data) + ',' + str(df_main_id) + ',' + '\'' + expired_at + '\'' + '),'
    sql = sql[0:len(sql) - 1]
    print(sql)
    sql_insert(sql=sql)


class deposit:

    def _read_excel(self):
        path = r'C:\解密文件\湛卢一级测试用例-test.xlsx'
        df = pd.read_excel(path,sheet_name=1)
        return df

    def _insert_data(self,df=None):
        if df is not None and len(df) > 0:
           #去掉用例标题空行
           no_empty_df = df.dropna(subset=['用例标题'],how='any')
           #获取标题的list集合
           title_list = no_empty_df.columns.values.tolist()
           # 遍历no_empty_df逐条插入数据
           key_value = []
           for index, row in no_empty_df.iterrows():
               df_main_id = 0
               for title in title_list:
                   if 'table_main' == title:
                       #插入第一张主表的数据，返回多条数据的主表子表关联主键
                        title = row[title]
                        key = row['测试用例key']
                        df_main = _insert_main_table_data(title,key)
                        df_main_id = df_main['id'][0]
                        key_value.append(df_main['key'][0])
                   if title.find('table_main_sub') >=0:
                         #插入第一张主表关联的子表数据
                         title = row[title]
                         _insert_main_table_sub_data(title,df_main_id)
           no_empty_df['keyValue'] = key_value
           return no_empty_df


    def _write_excel(self,df=None):
        path = 'C:\\Users\\杨也晰\\Desktop\\测试用例\\测试用例数据重生成.xlsx'
        if df is not None and len(df) > 0:
            df.to_excel(path)
        return path

    def _read_excel_run_process(self,path):
        df = pd.read_excel(path)
        code_array = []
        #跑代码的实际结果
        actual_reslut_array = []
        #与预期结果对比是否通过
        is_pass_array = []
        for index, row in df.iterrows():
            code = str(row['测试模块'])
            code_array.append(code)
            field = row['用例标题']
            expect_result = row['预期测试结果'].split('=')[1]
            params = eval(row['keyValue'])
            user_name = ''
            id_card_no = ''
            phone = ''
            #运行代码生成的用例字段结果
            value = ''
            for key, value in params.items():
                if key in ['user_name', 'unique_name', 'name']:
                    user_name = value
                if key in ['id_card_no','unique_id_no']:
                    id_card_no = value
                if key in ['phone']:
                    phone = value
            res = translate(code_array, user_name=user_name, id_card_no=id_card_no, phone=phone)
            for key in res:
                if field==key:
                    value = res[key]
                    actual_reslut_array.append(value)
            if str(value)==expect_result:
                is_pass_array.append("true")
            else:
                is_pass_array.append("false")
        df['实际测试结果'] = actual_reslut_array
        df['是否通过'] = is_pass_array
        df.to_excel(path)








