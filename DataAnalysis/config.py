from . import logging, DA_DIR, csv, SPREADSHEET_DIR, os

LOGGING_FILE = DA_DIR / 'all_logs.log'
ERROR_LOGGING_FILE = DA_DIR / 'error_logs.log'


LOGGING_CONFIG = {
    'version': 1,
    'disabled_existing_loggers': False,
    'formatters': {
        'generic': {
            'format': '%(asctime)s|%(levelname)-8s(%(filename)s)| %(message)s'
        },
        'error': {
            'format': '\n%(asctime)s|%(levelname)-8s| %(message)s\n%(pathname)s'
        }
    },
    'handlers': {
        'file': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': LOGGING_FILE,
            'mode': 'w',
            'formatter': 'generic'
        },
        'error_file': {
            'level': 'WARNING',
            'class': 'logging.FileHandler',
            'filename': ERROR_LOGGING_FILE,
            'mode': 'w',
            'formatter': 'error'

        },
        'console': {
            'level': 'WARNING',
            'class': 'logging.StreamHandler',
            'formatter': 'error'
        }
    },
    'loggers': {
        'standard': {
            'handlers': ['file', 'console', 'error_file'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger('standard')


def get_can_data(directory, filename) -> list[list[str]]:
    with open(f'{directory}/{filename}', 'r') as fn:
        hdr = fn.readline().strip().split(',')+['Box']
        format_with_box_name = [hdr]+[info+[filename[:-4]] for info in csv.reader(fn)]
        return format_with_box_name

def write_data(destination_file_name:str, data:list[dict[str,str]], first_time:bool):
    destination = SPREADSHEET_DIR / f'{destination_file_name}.csv'
    with open(destination, 'a') as fn:
        wtr = csv.writer(fn, lineterminator='\n')
        
        if first_time:
            wtr.writerows(data)
        else:
            wtr.writerows(data[1:])
        
    logger.info(f'done appending Can Data for Box {data[2][-1]} to {destination}')
    return

def format_2023_can_data():
    dir = os.path.abspath(SPREADSHEET_DIR/'2023_can_data_by_box')
    subdirs = os.listdir(dir)

    def _determine_first_time(index:int) -> bool:
        if index>0:
            return False
        return True

    for filename in subdirs:
        ft = _determine_first_time(subdirs.index(filename))
        cd = get_can_data(dir, filename)
        write_data('all_can_data', cd, ft)
    print('done combining all 2023 can data')
    return        

