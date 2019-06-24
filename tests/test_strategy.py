from logger.logger_util import LoggerUtil

LoggerUtil().logger(__name__)


def test_replace():
    value = '1, 3'
    r = value.replace('[', '').replace(')', '')
    print(int(r.split(',')[1]))
    print(int(r.split(',')[1]) // 12)
