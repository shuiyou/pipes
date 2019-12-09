# -*- coding: utf-8 -*-
# @Time : 2019/12/9 1:17 PM
# @Author : lixiaobo
# @Site : 
# @File : config_controller.py
# @Software: PyCharm
import json

from flask import Blueprint

from service.base_type_service import BaseTypeService

base_type_api = Blueprint('account_api', __name__)


@base_type_api.route("/base-type", methods=['GET'])
def base_type_mapping_info():
    base_type_service = BaseTypeService(None)
    return json.dumps(base_type_service.BASE_TYPE_MAPPING)
