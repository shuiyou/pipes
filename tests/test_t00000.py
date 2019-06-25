import pandas as pd

from mapping.t00000 import T00000
from mapping.t01001 import T01001
from mapping.t02001 import T02001


def test_base_info():
    t = T00000()
    t.run(user_name='刘劭卓', id_card_no='430105199106096118', phone='11111111111', user_type='PERSONAL')
    print(t.variables)
