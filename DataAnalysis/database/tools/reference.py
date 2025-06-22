from ... import logging, pd, generate
from ...config import DB_DIR, DB_CONFIG_DIR

from ..utils.registry import DatabaseRegistry


logger = logging.getLogger('standard')
db_reg = DatabaseRegistry()

rdf = DB_CONFIG_DIR / 'reference_data.csv'


def upload_reference_data(reference_data_file_path:str=str(rdf)):
    """Creates reference table of abbreviations based on initial data.
    
    Data specific reference types include: Flavors, Location. Additional types 
    include: Development, Administrative, Exceptions
    
    """
    assert reference_data_file_path.endswith('.csv'), f'Must supply a path to a CSV file, not : {reference_data_file_path}'
    reference_df = pd.read_csv(reference_data_file_path)

    ids = [generate(size=7) for _ in range(len(reference_df))]
    reference_df.insert(0, 'id', ids)


    db = db_reg.get_instance('master')
    conn, _ = db.create_connection()

    reference_df.to_sql('reference', con=conn, if_exists='replace', index=False)
    db.close_commit(conn)
    logger.info(f'Created reference table in the master database')
    return
