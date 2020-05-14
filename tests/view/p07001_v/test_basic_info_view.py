from resource.v07001_v.test_util import read_file, run
from view.v41001 import V41001


def test01():
    ps = V41001()
    params=read_file('../../resource/v07001_v/test_basic_info_view01.json')
    run(ps, params)
    assert ps.variables["name"]=="吴金龙"
    assert ps.variables["certificate_no"]=="350583197301156659"
    assert ps.variables["report_no"]=="458010469446324224"
    assert ps.variables["report_time"]=="2020-02-27"

