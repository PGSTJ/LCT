from . import logging, pathlib

# Date Time formats
PRIM_DATE_FORMAT = '%m/%d/%Y' # default date format
ALT_DATE_FORMAT = '%B %d, %Y'
FILE_DATE_FORMAT = '%m%d%Y'
PRIM_DATETIME_FORMAT = '%m/%d/%Y %H:%M:%S' # default datetime format

ALL_DATETIME_FORMATS = {
    'PRIM_DATE': PRIM_DATE_FORMAT,
    'ALT_DATE': ALT_DATE_FORMAT,
    'FILE_DATE': FILE_DATE_FORMAT,
    'PRIM_DATETIME': PRIM_DATETIME_FORMAT
}

DA_DIR = pathlib.Path(__file__).parent

DATABASE_UTIL_DIR = DA_DIR / 'database'
DB_DIR = DA_DIR / 'databases'

LOGS_DIR = DA_DIR / 'logs'
LOGGING_FILE = LOGS_DIR / 'generic.log'
ERROR_LOGGING_FILE = LOGS_DIR / 'errors.log'


# (Default) Config and output directories 
EXTERNAL_DATA_DIR = pathlib.Path(r'C:\Users\tmalo\Desktop\La Croix Data')
DB_CONFIG_DIR = DATABASE_UTIL_DIR / 'config_sheets'
DEFAULT_PROCESSING_OUTPUT_DIR = EXTERNAL_DATA_DIR / 'processed_data'





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

