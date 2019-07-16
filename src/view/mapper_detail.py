# -*- coding: utf-8 -*-
import importlib
# from app import logger
from numpy import median

from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer, numpy_to_int

logger = LoggerUtil().logger(__name__)

# 标识决策引擎交互完成。
STRATEGE_DONE = 'fffff'


def _get_codes_by_product_code(product_code):
    # TODO: 需要根据产品编码配置对应的code
    return ['12001', '10001', '13001', '17001', '07001', '09001']


def extension_variables(variables):
    """
    构建展示变量
    :param variables:
    :return:
    """
    extension = {
        'apply_bank_7d': 0,
        'apply_bank_1m': 0,
        'apply_bank_3m': 0,
        'apply_sloan_7d': 0,
        'apply_sloan_1m': 0,
        'apply_sloan_3m': 0,
        'apply_p2p_7d': 0,
        'apply_p2p_1m': 0,
        'apply_p2p_3m': 0,
        'apply_confin_7d': 0,
        'apply_confin_1m': 0,
        'apply_confin_3m': 0,
        'apply_other_7d': 0,
        'apply_other_1m': 0,
        'apply_other_3m': 0,
        'apply_bank_6m': 0,
        'apply_bank_12m': 0,
        'apply_bank_his': 0,
        'apply_sloan_6m': 0,
        'apply_sloan_12m': 0,
        'apply_sloan_his': 0,
        'apply_p2p_6m': 0,
        'apply_p2p_12m': 0,
        'apply_p2p_his': 0,
        'apply_confin_6m': 0,
        'apply_confin_12m': 0,
        'apply_confin_his': 0,
        'apply_other_6m': 0,
        'apply_other_12m': 0,
        'apply_other_his': 0
    }
    anti_apply_bank_7d = variables['anti_apply_bank_7d']
    net_apply_bank_7d = variables['net_apply_bank_7d']
    oth_loan_apply_bank_7d = variables['oth_loan_apply_bank_7d']
    variables.update(extension)

def translate_for_report_detail(product_code, user_name=None, id_card_no=None, phone=None, user_type=None):
    """
    根据产品编码对应的excel文件从Gears数据库里获取数据做转换处理。
    处理后的结果作为决策需要的变量。
    :return: 一个dict对象包含产品所需要的变量
    """

    variables = {}

    try:
        codes = _get_codes_by_product_code(product_code)
        for c in codes:
            trans = get_transformer(c)
            trans_result = trans.run(user_name=user_name,
                                     id_card_no=id_card_no,
                                     phone=phone,
                                     user_type=user_type)
            variables.update(trans_result['variables'])
    except Exception as err:
        logger.error(">>> translate error: " + str(err))
        raise ServerException(code=500, description=str(err))

    extension_variables(variables)
    # 转换类型，这样解决tojson的问题
    numpy_to_int(variables)
    return {
        'variables': variables
    }


def get_transformer(code) -> Transformer:
    """
    根据code构建对应的转换对象
    :param code:
    :return:
    """
    try:
        model = importlib.import_module("view.v" + str(code))
        api_class = getattr(model, "V" + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Transformer()
