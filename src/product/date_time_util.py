# @Time : 2020/4/28 3:55 PM 
# @Author : lixiaobo
# @File : date_time_util.py 
# @Software: PyCharm

from  datetime import datetime


def after_ref_date(year, month, ref_year, ref_month):
    if ref_month < 1:
        ref_month = 12 + ref_month
        ref_year = ref_year - 1
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
        return year-1,12+month-n

#获取date前n年的年份和月份
def before_n_year(date,n):
    year = date.year
    month = date.month
    return year-n,month

#获取date前n个月的日期
def before_n_month_date(source_date,n):
    year=source_date.year
    month=source_date.month
    day=source_date.day
    if month-n>0:
        target_year=year
        target_month=month-n
    else:
        target_year=year-1
        target_month=12+month-n
    target_date=source_date.replace(target_year,target_month,day)
    return target_date

def after_n_month_date(date,n):
    year = date.year
    month = date.month
    day = date.day
    if month+n < 13:
        target_year = year
        target_month = month - n
    else:
        target_year = year+1
        target_month = month + n - 12
    return date.replace(target_year, target_month, day)

#获取date前n年的日期
def before_n_year_date(date,n):
    year=date.year
    month=date.month
    day=date.day
    return date.replace(year-n,month,day)

def date_to_timestamp(source_date):
    date_str=str(source_date)[0:10]
    return datetime.strptime(date_str, "%Y-%m-%d")

