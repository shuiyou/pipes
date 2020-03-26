# -*- coding: utf-8 -*-
# @Time : 2019/12/9 1:05 PM
# @Author : lixiaobo
# @Site : 
# @File : FileUtil.py
# @Software: PyCharm

from file_utils.files import read_content


def get_config_content(file_name):
    return read_content(file_name, __file__)
