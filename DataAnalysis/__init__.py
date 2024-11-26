import pathlib
import csv
import os
import logging
import logging.config
import datetime
import io
import re
import traceback
import json
from typing import Literal

import numpy as np
import pandas as pd

DA_DIR = pathlib.Path(__file__).parent

SPREADSHEET_DIR = DA_DIR / 'spreadsheets'
DATABASE_DIR = DA_DIR / 'database'

logger = logging.getLogger('standard')

DATE_FORMAT = '%m/%d/%Y' # default date format
ALT_DATE_FORMAT = '%B %d, %Y'


def read_json_data(path_to_data:str) -> dict[str,str]:
    """"""
    with open(path_to_data, 'r') as fn:
        data = json.load(fn)
        return data

def read_csv_data(path_to_data:str, output_type:Literal['dict','list']) -> list[dict[str,str]] | list[list[str]]:
    """Reads all data by default"""
    with open(path_to_data, 'r') as fn:
        header = fn.readline().strip().split(',')

        if output_type == 'dict':
            data = csv.DictReader(fn, header)
        elif output_type == 'list':
            data = csv.reader(fn)

        return [row for row in data]

def convert_all_box_data() -> list[dict[str,str]]:
    data = read_csv_data(SPREADSHEET_DIR/'all_box_data.csv', 'dict')
    return data
    # return pd.DataFrame(data)

def convert_all_can_data() -> list[dict[str,str]]:
    data = read_csv_data(SPREADSHEET_DIR / 'all_can_data.csv', 'dict')
    return data
    # return pd.DataFrame(data)


ALL_BOX_DATA_DF = convert_all_box_data()
ALL_CAN_DATA_DF = convert_all_can_data()






