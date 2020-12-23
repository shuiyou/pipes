from fileparser.trans_flow.trans_config import MAX_TITLE_NUMBER, TRANS_TIME_PATTERN, \
    TRANS_AMT_PATTERN, TRANS_BAL_PATTERN, TRANS_OPNAME_PATTERN
import pandas as pd
import re

from logger.logger_util import LoggerUtil

logger = LoggerUtil().logger(__name__)


class TransXls:
    """
    若上传流水文件类型是xls*文件，将调用此类进行数据读取
    author:汪腾飞
    create_time:20201222
    """
    def __init__(self, file):
        self.file = file
        # self.param = param
        self.sheet_name = None
        self.title = None
        self.title_params = {}
        self.trans_data = None
        self.basic_status = True
        self.resp = {
            "resCode": "0",
            "resMsg": "成功",
            "data": {
                "warningMsg": []
            }
        }

    def process(self):
        self.sheet_name, self.title = self._find_title()
        if self.sheet_name is None or self.title is None:
            self.basic_status = False
            self.resp['resCode'] = '20'
            self.resp['resMsg'] = '解析失败'
            self.resp['data']['warningMsg'] = ['上传失败,无法找到标题行,流水文件内容有误']
            return
        self.trans_data = self._convert_to_dataframe()

    def _find_title(self):
        """
        从文件中的所有sheet中寻找存在流水标题行的文件，如果存在则跳出循环
        :return:
        """
        title_df = pd.read_excel(self.file, nrows=MAX_TITLE_NUMBER, header=None, sheet_name=None)
        # 遍历所有sheet
        for k, v in title_df.items():
            if v.shape[0] == 0:
                continue
            max_len = 0  # 最大列
            title = -1  # 标题行号
            cnt = 0  # 遍历行数计数
            for row in v.itertuples():
                temp = [str(x) for x in row if re.sub(r'\s', '', str(x)) != '']
                temp_len = len(temp)
                string = ''.join(temp)
                if re.search(TRANS_TIME_PATTERN, string) and \
                        re.search(TRANS_AMT_PATTERN, string) and \
                        re.search(TRANS_BAL_PATTERN, string) and \
                        re.search(TRANS_OPNAME_PATTERN, string):
                    if temp_len > max_len:
                        title = cnt
                        max_len = temp_len
                cnt += 1
            if title != -1:
                return k, title
        return None, None

    def _convert_to_dataframe(self):
        df = None
        try:
            df = pd.read_excel(self.file, header=self.title, sheet_name=self.sheet_name)
            self.basic_status = True
        except:
            self.basic_status = False
            self.resp['resCode'] = '1'
            self.resp['resMsg'] = '失败'
            self.resp['data']['warningMsg'] = ['文件读取异常']
        return df


# file_path = r'E:\installplace\git project\pipes\tests\resource\trans_flow' \
#             r'\temp_test\建设1.xlsx'
# x = TransXls(file_path)
# x.process()
# print(x.trans_data.shape)
