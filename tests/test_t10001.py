from mapping.t10001 import T10001


def test__ovdu_sco_1y():
    t10001 = T10001()
    t10001.run('赛达尔', '332205199801021324')
    print(t10001.variables)
