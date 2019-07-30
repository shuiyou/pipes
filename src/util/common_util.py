import time


def to_string(obj):
    if obj is None:
        return ''
    return str(obj)


def format_timestamp(obj):
    if obj is not None:
        return time.strftime('%Y-%m-%d %H:%M:%S', obj)
    else:
        return ''
