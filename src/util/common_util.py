import traceback

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


def to_string(obj):
    if obj is None:
        return ''
    return str(obj)


def format_timestamp(obj):
    if obj is not None:
        return obj.strftime('%Y-%m-%d')
    else:
        return ''


def exception(describe):
    def robust(actual_do):
        def add_robust(*args, **keyargs):
            try:
                return actual_do(*args, **keyargs)
            except Exception as e:
                logger.error(describe)
                logger.error(traceback.format_exc())
        return add_robust
    return robust