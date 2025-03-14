"""Initial Sequence for database creation



"""

from .. import ALL_BOX_DATA_DF, ALL_CAN_DATA_DF
from . import REFERENCE_DATA
from .utils import Box, Can, logging, Database, check_abbreviation_existence

logger = logging.getLogger('standard')
database = Database()
database.create_tables()

def upload_can_box_data():
    """Uploads box and can data to database"""
    for data in ALL_BOX_DATA_DF:
        box_obj = Box(data['bid'], data)
        box_obj.db_insert()
    print('done uploading box data to DB')

    for data in ALL_CAN_DATA_DF:
        can_obj = Can(data['Can'], data)
        can_obj.db_insert()
    print('done uploading can data to DB')
    logger.info('done uploading can data to DB')
    return

def upload_reference_data():
    """Creates reference table of abbreviations based on initial data.
    
    Data specific reference types include: Flavors, Location. Additional types 
    include: Development, Administrative, Exceptions
    
    """
    conn, curs = database._create_connection()
    for data in REFERENCE_DATA:
        curs.execute('INSERT INTO reference_data(abbreviation, definition, type) VALUES (?,?,?)', (tuple(data)))
    logger.info('Successfully created reference data table')
    database._close_commit(conn)

def run(fill_data:bool=True):
    upload_reference_data()
    if fill_data:
        upload_can_box_data()
    return


if __name__ == '__main__':
    run()