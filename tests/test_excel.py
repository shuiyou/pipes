from mapping import excel_reader


def test_config_read():
    df = excel_reader.read_excel_config("湛泸一级清洗数据.xlsx")
    print(df.columns)
    print(df['变量名'])
