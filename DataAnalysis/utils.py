from . import Literal, csv, json, datetime, yaml, pickle
from .config import ALL_DATETIME_FORMATS, logging

logger = logging.getLogger('standard')

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
    

def get_current_time(format:Literal['PRIM_DATE', 'FILE_DATE', 'PRIM_DATETIME']):
    return datetime.datetime.now().strftime(ALL_DATETIME_FORMATS[format])


def read_yaml_data(config_file_path:str) -> dict|list[dict]:
    """ Reads contents of all documents in a .yaml config file and returns as a dict"""

    with open(config_file_path, 'r') as fn:
        data = [i for i in yaml.load_all(fn, Loader=yaml.Loader)]

    if len(data) == 1:
        logger.info(f'Only one YAML document found in {config_file_path}')
        return data[0]
    else:
        logger.info(f'{len(data)} YAML documents found in {config_file_path}')
        return data
    

class PickleHandler:
    """ Helper class to handle (un)pickling data and associated tracking data """

    def __init__(self):
        pass


    def load_pickle(self, file_path:str):
        assert file_path.endswith('.pkl'), f'Unsupported file type: {file_path}'
        with open(file_path, 'rb') as fn:
            data = pickle.load(fn)
            logger.info(f'Successfully loaded data from {file_path}')
            return data
        
    def save_pickle(self, data, file_path:str):
        assert file_path.endswith('.pkl'), f'Unsupported file type: {file_path}'

        with open(file_path, 'wb') as fn:
            pickle.dump(data, fn)
            logger.info(f'Successfully saved data to {file_path}')
        return