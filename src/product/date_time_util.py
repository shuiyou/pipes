# @Time : 2020/4/28 3:55 PM 
# @Author : lixiaobo
# @File : date_time_util.py 
# @Software: PyCharm

from  datetime import datetime


def after_ref_date(year, month, ref_year, ref_month):
    if year > ref_year:
        return True
    elif year < ref_year:
        return False
    else:
        return ref_month <= month

#获取date前n个月的年份和月份
def before_n_month(date,n):
    year=date.year
    month=date.month
    if month-n>0:
        return year,month-n
    else:
        return year-1,13-n

