# @Time : 2020/4/28 3:55 PM 
# @Author : lixiaobo
# @File : date_time_util.py 
# @Software: PyCharm


def after_ref_date(year, month, ref_year, ref_month):
    if ref_month < 1:
        ref_month = 12 + ref_month
    if year > ref_year:
        return True
    elif year < ref_year:
        return False
    else:
        return ref_month <= month
