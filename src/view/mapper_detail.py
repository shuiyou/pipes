# -*- coding: utf-8 -*-
import importlib

# from app import logger
from exceptions import ServerException
from logger.logger_util import LoggerUtil
from mapping.tranformer import Transformer

logger = LoggerUtil().logger(__name__)

# 标识决策引擎交互完成。
STRATEGE_DONE = 'fffff'


def _get_codes_by_product_code(product_code):
    # TODO: 需要根据产品编码配置对应的codeß
    return ['12001', '10001', '13001', '17001', '07001', '09001', '13001', '00000']


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
            variables.update(trans_result)
    except Exception as err:
        logger.error(">>> translate error: " + str(err))
        raise ServerException(code=500, description=str(err))

    return variables


def get_transformer(code) -> Transformer:
    """
    根据code构建对应的转换对象
    :param code:
    :return:
    """
    try:
        model = importlib.import_module("view.v" + str(code))
        api_class = getattr(model, "T" + str(code))
        api_instance = api_class()
        return api_instance
    except ModuleNotFoundError as err:
        logger.error(str(err))
        return Transformer()
