from resource.v07001_v.test_util import read_file, run
from view.v41001 import V41001


def test01():
    ps = V41001()
    params=read_file('../../resource/v07001_v/test_basic_info_view01.json')
    run(ps, params)
    assert ps.variables["default_type"]==['0301','呆账']