from ... import Literal, logging, pd, generate, os, np, datetime
from ...config import DB_DIR

from ..utils.registry import DatabaseRegistry, Database

logger = logging.getLogger('standard')
db_reg = DatabaseRegistry(base_database_dir=DB_DIR)

GENERAL_PARAMETERS = [
    'avg_can_mass',
    'avg_can_volume',
    'avg_empty_can_mass', 
    'avg_empty_can_volume',
    'finish_threshold'
]



def default_fill_general_table():
    dyn_db = db_reg.get_instance('dynamic_analyses')

    table_data = {
        'parameters': GENERAL_PARAMETERS,
        'value': np.zeros(len(GENERAL_PARAMETERS))
    }

    td_df = pd.DataFrame(table_data)

    conn, _ = dyn_db.create_connection()
    td_df.to_sql(name='general', con=conn, if_exists='replace', index=False)
    dyn_db.close_commit(conn)

    logger.info(f'Filled DynamicAnalyses General table with default parameters: {table_data['parameters']}')

def update_table_general():
    """ Updates the parameters in the general table 
    
    
    """
    raw_data_db = db_reg.get_instance('raw_data')

    # update empty can measurements
    ec_measurements = raw_data_db.get_data(['initial_mass', 'initial_volume', 'empty_can_mass'], 'can_data')

    # Remove empty values and calculate volume (fl oz) for each remaining mass
    valid_measurements = ec_measurements[ec_measurements['empty_can_mass'].notna()].reset_index()
    empty_can_vol = valid_measurements['empty_can_mass'].apply(lambda x: round(x / 29.5, 2))
    valid_measurements.insert(len(valid_measurements.columns), 'empty_can_volume', empty_can_vol)

    map = {
        'initial_mass':'avg_can_mass',
        'initial_volume': 'avg_can_volume'
    }

    # get dynamic database and update general table
    dyn_db = db_reg.get_instance('dynamic_analyses')
    for cols in valid_measurements.columns:
        if cols == 'finish_threshold':
            continue
        average = round(valid_measurements[cols].mean(), 2)
        parameter = map[cols] if cols in map else f'avg_{cols}'

        set = {'value':average}
        where = {'parameters':parameter}

        dyn_db.update_values('general', set, where)

    # finish threshold is based on average mass/volume for initial and empty cans


    print(f'Updated the general Dynamic Analysis table')

    return



def update_dynamic_analyses():
    """
    
    
    """

