from resource.v07001_v.test_util import read_file, run
from view.v41001 import V41001


def test_null():
    ps = V41001()
    run(ps, "吴金龙", "350583197301156659", "458010469446324224")
    assert ps.variables["default_type"]==['0301','呆账']