"""Initial Sequence for database creation



"""

from .. import ALL_BOX_DATA_DF, ALL_CAN_DATA_DF
from . import pd, datetime, DA_DIR
from .utils import Box, Can, logging, Database, check_abbreviation_existence, SPREADSHEET_DIR

logger = logging.getLogger('standard')
database = Database()
# database.create_tables()

FILE_DB_NAME_MAP = {
    'all_box_data':'boxes_all',
    'box_by_flavor':'boxes_flavor',
    'all_can_data':'can_data'
}

custom_version = None
VERSION = datetime.datetime.now().strftime('%m%d%Y') if custom_version is None else custom_version


def upload_lacroix_data():
    """ Uploads box and can data to database """
    conn, curs = database.create_connection()

    data_dfs = {dataname:pd.read_csv(SPREADSHEET_DIR / rf'processed/{dataname}_{VERSION}.csv') for dataname in FILE_DB_NAME_MAP}
    for filename in data_dfs:
        data_dfs[filename].to_sql(name=FILE_DB_NAME_MAP[filename], con=conn, if_exists='append', index=False)
    
    return

def upload_can_box_data():
    """"""
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
    conn, curs = database.create_connection()
    ref_data_file = DA_DIR / 'database/reference_data.csv'
    ref_data_df = pd.read_csv(ref_data_file)
    ref_data_df.to_sql('reference_data', conn, if_exists='append', index=False)
    # database.close_commit(conn)


    # for data in REFERENCE_DATA:
    #     curs.execute('INSERT INTO reference_data(abbreviation, definition, type) VALUES (?,?,?)', (tuple(data)))
    logger.info('Successfully created reference data table')
    database.close_commit(conn)
    return

def run(recreate_tables:bool=False, fill_data:bool=True):
    if recreate_tables:
        database._recreate_tables()

    upload_reference_data()
    if fill_data:
        upload_lacroix_data()
    return


if __name__ == '__main__':
    run()