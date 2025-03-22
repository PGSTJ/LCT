"""Initial Sequence for database creation



"""

from .. import pd, datetime, generate
from ..config import DA_DIR, SPREADSHEET_DIR, DB_CONFIG_DIR
from .utils import Box, Can, logging, Database, DatabaseRegistry

logger = logging.getLogger('standard')
database = None
# database.create_tables()

FILE_DB_NAME_MAP = {
    'all_box_data':'boxes_purchase',
    'box_by_flavor':'boxes_flavor',
    'all_can_data':'can_data'
}

custom_version = None
VERSION = datetime.datetime.now().strftime('%m%d%Y') if custom_version is None else custom_version


def upload_lacroix_data(database_registry:DatabaseRegistry):
    """ Uploads box and can data to database """
    db = database_registry.get_instance('raw_data')
    
    conn, _ = db.create_connection()

    data_dfs = {dataname:pd.read_csv(SPREADSHEET_DIR / rf'processed/{dataname}_{VERSION}.csv') for dataname in FILE_DB_NAME_MAP}
    for filename in data_dfs:
        data_dfs[filename].to_sql(name=FILE_DB_NAME_MAP[filename], con=conn, if_exists='append', index=False)
    db.close_commit(conn)
    
    return


def upload_reference_data(database_registry:DatabaseRegistry):
    """Creates reference table of abbreviations based on initial data.
    
    Data specific reference types include: Flavors, Location. Additional types 
    include: Development, Administrative, Exceptions
    
    """
    reference_data_file = DB_CONFIG_DIR / 'reference_data.csv'
    reference_df = pd.read_csv(reference_data_file)

    ids = [generate(size=7) for _ in range(len(reference_df))]
    reference_df.insert(0, 'id', ids)


    db = database_registry.get_instance('master')
    conn, _ = db.create_connection()

    reference_df.to_sql('reference', con=conn, if_exists='replace', index=False)
    db.close_commit(conn)
    logger.info(f'Created reference table in the master database')
    return

def run(recreate_tables:bool=False, fill_data:bool=True):
    dbr = DatabaseRegistry()
    if recreate_tables:
        dbr._reset_all_db_tables()

    upload_reference_data(dbr)
    if fill_data:
        upload_lacroix_data(dbr)
    return


if __name__ == '__main__':
    run()