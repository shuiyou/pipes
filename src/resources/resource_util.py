# -*- coding: utf-8 -*-
# @Time : 2019/12/9 1:05 PM
# @Author : lixiaobo
# @Site : 
# @File : FileUtil.py
# @Software: PyCharm
import os
from os.path import abspath, dirname


def read_content(file_name):
    f = None
    try:
        BASE_DIR = dirname(abspath(__file__))
        resource_full_path = os.path.join(BASE_DIR, file_name)
        f = open(resource_full_path, 'r', encoding='UTF-8')
        str_content = f.read()
    finally:
        if f is not None:
            f.close()
    return str_content
