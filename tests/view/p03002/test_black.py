import json

from file_utils.files import file_content

from view.p03002.black_union import Black


def test_001():
    ps = Black()
    ps.full_msg = json.loads(file_content(r"C:/workspace/pipes/tests/resource", "unin_level1_001.json"))
    ps.run()
    print(ps.variables)