
from ... import Literal, logging, pd, generate, os
from ...config import DB_DIR

from utils.registry import Database, DatabaseRegistry


logger = logging.getLogger('standard')
db_reg = DatabaseRegistry(base_database_dir=DB_DIR)





def update_static_analyses():
    """ Compares box_flavor / can ids with static_analyses.db and fill/update for any missing IDs


    
    """
    # link up db home dir and conv db tables back to df
    # calculate each column/table individually then combine into one DF
    raw_data = db_reg.get_instance('raw_data')
    static_anlys = db_reg.get_instance('static_analyses')

    conn, curs = raw_data.create_connection()

    # rd_dfs = [pd.read_sql(f'SELECT * FROM {tablename}', conn) for tablename in raw_data.tables.keys()]
    # print(f'{rd_dfs = }')
    for tblename in raw_data.tables.keys():
        print(tblename)


