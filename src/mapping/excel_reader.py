import os

import pandas as pd


def read_excel_config(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    excel_file = os.path.join(root_dir, 'mapping', file_name)
    return pd.read_excel(excel_file)
