# -*- coding: utf-8 -*-
import os

import pandas as pd


def translate(product_code):
    """
    根据产品编码对应的excel文件从Gears数据库里获取数据做转换处理。
    处理后的结果作为决策需要的变量。
    :param product_code:
    :return: 一个dict对象包含产品所需要的变量
    """
    product_df = read_product(product_code)

    pass


def read_product(product_code):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    excel_file = os.path.join(root_dir, 'product', product_code + '.xlsx')
    return pd.read_excel(excel_file, usecols=[0, 1], dtype={'code': str})
