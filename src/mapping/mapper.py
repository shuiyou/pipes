# -*- coding: utf-8 -*-
import importlib
import os

import pandas as pd

from app import logger
from mapping.tranformer import Transformer


def translate(product_code, user_name=None, id_card_no=None, phone=None):
    """
    根据产品编码对应的excel文件从Gears数据库里获取数据做转换处理。
    处理后的结果作为决策需要的变量。
    :return: 一个dict对象包含产品所需要的变量
    """
    product_df = read_product(product_code)
    codes = product_df['code'].unique()
    variables = {}
    for c in codes:
        trans = get_transformer(c)
        trans.transform(user_name=user_name,
                        id_card_no=id_card_no,
                        phone=phone)
        variables.update(trans.variables)

    return variables


def read_product(product_code):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    excel_file = os.path.join(root_dir, 'product', product_code + '.xlsx')
    return pd.read_excel(excel_file, usecols=[0, 1], dtype={'code': str})


def get_transformer(code) -> Transformer:
    """
    根据code构建对应的转换对象
    :param code:
    :return:
    """
    try:
        model = importlib.import_module("mapping.t" + str(code))
        api_class = getattr(model, "T" + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Transformer()
