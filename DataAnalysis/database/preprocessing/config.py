
from typing import Literal, TypeAlias
from nanoid import generate
import traceback

from .. import (
    logging, csv, os, pd, re, io, traceback, datetime,
    SPREADSHEET_DIR
)

logger = logging.getLogger('standard')

CSV_DATA_DIR = os.path.join(SPREADSHEET_DIR, 'csv_raw')
MD_DATA_DIR = os.path.join(SPREADSHEET_DIR, 'md_raw')

# BoxDS corresponds to box source data csv header format
# BoxA and BoxF correspond to box_all and box_flavor database table column format 
HEADERS:dict[str,list[str]] = {
    "Can": [
        'can_id',
        'box_id',
        'initial_mass',
        'initial_volume',
        'final_mass',
        'final_volume',
        'finish_status',
        'percent_mass_remaining',
        'percent_volume_remaining'
    ],
    "BoxDS": [
        'og_id',
        'flavor',
        'purchase_date',
        'price',
        'location',
        'start_date',
        'finish_date',
        'DV',
        'TTS'
    ],
    "BoxA": [
        'box_id',
        'purchase_date',
        'price',
        'location',
        'og_id'
    ],
    "BoxF": [
        'bfid',
        'box_id',
        'flavor',
        'start_date',
        'finish_date',
    ]

}

DATE_FORMAT = '%m/%d/%Y'
FILE_DATE_FORMAT = '%m%d%Y'
VERSION = datetime.datetime.now().strftime(FILE_DATE_FORMAT)

BOX_ALL_EXPORT = os.path.join(SPREADSHEET_DIR, 'processed', f'all_box_data_{VERSION}.csv')
BOX_FLAVOR_EXPORT = os.path.join(SPREADSHEET_DIR, 'processed', f'box_by_flavor_{VERSION}.csv')
CAN_EXPORT = os.path.join(SPREADSHEET_DIR, 'processed', f'all_can_data_{VERSION}.csv')