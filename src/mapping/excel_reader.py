# -*- coding: utf-8 -*-

import os

import pandas as pd


def read_excel_config(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  ## 返回文件根目录
    excel_file = os.path.join(root_dir, 'mapping', file_name)  ## 连接目录与文件名
    return pd.read_excel(excel_file)
