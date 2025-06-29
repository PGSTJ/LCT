from . import logging, pathlib

# Date Time formats
ALL_DATETIME_FORMATS = {
    'PRIM_DATE': '%m/%d/%Y', # default date format
    'ALT_DATE': '%B %d, %Y',
    'FILE_DATE': '%m%d%Y',
    'PRIM_DATETIME': '%m/%d/%Y %H:%M:%S' # default datetime format
}

DA_DIR = pathlib.Path(__file__).parent

# modules
DATABASE_UTIL_DIR = DA_DIR / 'database'
DB_DIR = DA_DIR / 'databases'

LOGS_DIR = DA_DIR / 'logs'
ALL_LOGS = LOGS_DIR / 'all.log'
GENERAL_LOG_FILE = LOGS_DIR / 'generic.log'
ERROR_LOGGING_FILE = LOGS_DIR / 'errors.log'


# (Default) Config and output directories 
EXTERNAL_DATA_DIR = pathlib.Path(r'C:\Users\tmalo\Desktop\La Croix Data')
DB_CONFIG_DIR = DATABASE_UTIL_DIR / 'config_sheets'
SAVED_TABLE_DATA_DIR = DATABASE_UTIL_DIR / 'saved_table_data'
DEFAULT_PROCESSING_OUTPUT_DIR = EXTERNAL_DATA_DIR / 'processed_data'

# DB_DIR = EXTERNAL_DATA_DIR / 'databases'




LOGGING_CONFIG = {
    'version': 1,
    'disabled_existing_loggers': False,
    'formatters': {
        'generic': {
            'format': '%(asctime)s|%(levelname)-8s(%(filename)s)| %(message)s'
        },
        'error': {
            'format': '%(asctime)s|%(levelname)-8s|%(funcName)-8s| %(message)s\n%(pathname)s'
        }
    },
    'handlers': {
        'all_logs': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': ALL_LOGS,
            'mode': 'w',
            'formatter': 'generic'
        },
        'maintenance': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': GENERAL_LOG_FILE,
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
            'handlers': ['all_logs', 'maintenance', 'console', 'error_file'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}

logging.config.dictConfig(LOGGING_CONFIG)        

