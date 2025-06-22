from ... import Literal, logging, pd, generate, os, np, datetime
from ...config import DB_DIR

from ..utils.registry import DatabaseRegistry, Database

logger = logging.getLogger('standard')
db_reg = DatabaseRegistry()

GENERAL_PARAMETERS = [
    'avg_can_mass',
    'avg_can_volume',
    'avg_empty_can_mass', 
    'avg_empty_can_volume'
]



def default_fill_can_measurements():
    dyn_db = db_reg.get_instance('dynamic_analyses')

    table_data = {
        'parameters': GENERAL_PARAMETERS,
        'value': np.zeros(len(GENERAL_PARAMETERS))
    }

    td_df = pd.DataFrame(table_data)

    conn, _ = dyn_db.create_connection()
    td_df.to_sql(name='can_measurements', con=conn, if_exists='replace', index=False)
    dyn_db.close_commit(conn)

    logger.info(f'Filled DynamicAnalyses Can Measurements table with default parameters: {table_data['parameters']}')

def update_can_measurements():
    """ Updates the parameters in the can measurements table 
    
    
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

        dyn_db.update_values('can_measurements', set, where)

    # finish threshold is based on average mass/volume for initial and empty cans


    print(f'Updated the Can Measurements (Dynamic Analyses) table')

    return



def update_flavor_analysis(if_exists:Literal['fail', 'replace', 'append']='append', display_update:bool=False):
    """
    
    
    """

    static_analyses = db_reg.get_instance('static_analyses')
    raw_data = db_reg.get_instance('raw_data')
    
    static_df = static_analyses.get_data(['*'], 'box_analysis')
    flavor_df = raw_data.get_data(['id', 'flavor'], 'box_flavors')

    all_data = []
    for flavor, df in flavor_df.groupby('flavor'):
        bf_ids = df['id'].to_list()

        analyses = static_df[static_df['box_id'].isin(bf_ids)]

        
        
        package = { # NOTE all means are double mean
            'flavor': flavor,
            'total_purchased': len(df),
            'average_drink_velocity':analyses['drink_velocity'].mean(),
            'average_time_to_start':analyses['time_to_start'].mean(),
            'average_finish_rate': analyses['completion_percentage'].mean(),
            'average_pmr': analyses['average_pmr'].mean(), 
            'average_pvr': analyses['average_pvr'].mean()
        }

        all_data.append(package)

    
    df = pd.DataFrame(all_data)

    if display_update:
        print(df)

    dyn_db = db_reg.get_instance('dynamic_analyses')

    conn, _ = dyn_db.create_connection()
    df.to_sql('flavor_analysis', conn, index=False, if_exists=if_exists)
    dyn_db.close_commit(conn)

    logger.info(f'Updated flavor_analysis table of Dynamic Analyses')

    return




