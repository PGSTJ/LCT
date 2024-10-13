import pathlib
import csv
import os
import logging
import logging.config
import datetime
import io
import re

import numpy as np
import pandas as pd

DA_DIR = pathlib.Path(__file__).parent

SPREADSHEET_DIR = DA_DIR / 'spreadsheets'

logger = logging.getLogger('standard')


def read_data(path_to_data:str):
    """Reads all data by default"""
    with open(path_to_data, 'r') as fn:
        data = csv.DictReader(fn)
        return [row for row in data]

def convert_all_box_data():
    data = read_data(SPREADSHEET_DIR/'all_box_data.csv')
    return pd.DataFrame(data)

def convert_all_can_data():
    data = read_data(SPREADSHEET_DIR / 'all_can_data.csv')
    return pd.DataFrame(data)


ALL_BOX_DATA_DF = convert_all_box_data()
ALL_CAN_DATA_DF = convert_all_can_data()





