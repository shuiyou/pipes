from data.data_deposit import deposit
import pandas as pd


def test_data_deposit():
     ps = deposit()
     #读取excel
     read_excel_path = r'C:\解密文件\一级测试用例-16002.xlsx'
     df = ps._read_excel(read_excel_path)
     #插入测试用例数据
     df_write_excel = ps._insert_data(df=df)
     #重新生成带参数的测试用例excel
     writ_excel_path = 'C:\\Users\\杨也晰\\Desktop\\测试用例\\'
     path = ps._write_excel(path=writ_excel_path,df=df_write_excel)
     #读取新的excel跑对应的清洗逻辑，与预期值判断是否通过
     ps._read_excel_run_process(path=path)




def test_string_split():
     df_main_id = 308
     expired_at = '2030-12-20'
     value = 'info_court_excute_public.court_id;' \
             'info_court_excute_public.filing_time[0]="2016-07-01";' \
             'info_court_excute_public.execute_content[0]="金额:2500.00";' \
             'info_court_excute_public.filing_time[1]="2016-06-30";' \
             'info_court_excute_public.execute_content[1]="执行标的金额（万元）:0.2";' \
             'info_court_excute_public.filing_time[2]="2018-08-01";' \
             'info_court_excute_public.execute_content[2]="执行标的金额（万元）:0.1"'
     value = value.replace('\n','').replace('\r','')
     value_array = value.split(';')
     table_name = ''
     relation_mian_table_key = ''
     #字段种类
     field_count = 0
     field_array = []
     #描述有多少组数据
     data_group_count = 0
     sql = """
     insert into 
     """
     key_value_array =[]
     if len(value_array) > 0:
          table_name = value_array[0].split('.')[0]
          relation_mian_table_key = value_array[0].split('.')[1]
          for detail in value_array:
               if detail.find('[0]') >= 0:
                    field_count  = field_count + 1
                    field_array.append(detail.split('[0]')[0].split('.')[1])
          if field_count > 0:
               data_group_count = (len(value_array) - 1) / field_count
               data_group_count = int(data_group_count)

          for i in range(data_group_count):
               data = []
               for field in field_array:
                    for detail in value_array:
                         if detail.find(field+'['+str(i)+']') >=0:
                              data.append(detail.split('=')[1])
               key_value_array.append(data)
     sql += ' ' + table_name + ' ('
     sql += ','.join(field_array)
     sql += ','+ relation_mian_table_key
     sql += ',expired_at) values '
     for data in key_value_array:
          sql += '('+ ','.join(data) + ','+str(df_main_id) + ','+'\''+expired_at+ '\''+ '),'
     sql = sql[0:len(sql)-1]
     print(sql)






def test_string_array():
     a = ['11','22','33']
     b = ['aa','bb','cc']
     c = ['ee','ee',None]
     d = ['','','']
     df1 = pd.DataFrame({"key1":a,"key2":c})
     #df2 = pd.DataFrame({"我是":b})
     #df3 = pd.merge(df1,df2,on='key',how='left')
     #df3 = pd.concat([df1,df2],axis=1)
     df1 = df1.dropna(subset=['key2'],how='any')
     print(df1)


def test_to_dict():
     value = '{"unique_name":"房博","unique_id_no":"430211196310226521"}'
     value = eval(value)
     for key, value in value.items():
         if key in ['user_name','unique_name','name']:
              print(key+'---'+value)









