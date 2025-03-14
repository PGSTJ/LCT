from . import logging, DA_DIR

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

