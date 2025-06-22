
from ... import Literal, logging, pd, generate, os, np, datetime
from ...config import DB_DIR

from ..utils.registry import DatabaseRegistry, Database

logger = logging.getLogger('standard')
db_reg = DatabaseRegistry()

# threshold for a can to objectively be considered "finished"
# can be interpreted as direct numerical threshold for percent mass remaining
OFS_THRESHOLD = 0.015


# NOTE currently pulls all saved data from raw_data -> only works when fully reseting databases (pipeline preset 1)
# TODO need custom handling for post processing specific updates 
def update_static_analyses(if_exists:Literal['fail', 'replace', 'append']='append'):
    """ Compares box_flavor / can ids with static_analyses.db and fill/update for any missing IDs


    
    """
    # link up db home dir and conv db tables back to df
    # calculate each column/table individually then combine into one DF
    raw_data = db_reg.get_instance('raw_data')
    static_anlys = db_reg.get_instance('static_analyses')

    raw_conn, _ = raw_data.create_connection()
    rd_dfs = {tablename:pd.read_sql(f'SELECT * FROM {tablename}', raw_conn) for tablename in raw_data.tables.keys()}
    raw_data.close_commit(raw_conn)


    ca_data = update_can_analyses(rd_dfs['can_data'], display_updates=False)
    ba_data = update_box_analyses(rd_dfs, ca_data, display_updates=False)

    conn, _ = static_anlys.create_connection()
    ca_data.to_sql('can_analysis', conn, if_exists=if_exists, index=False)
    ba_data.to_sql('box_analysis', conn, if_exists=if_exists, index=False)
    static_anlys.close_commit(conn)

    logger.info('Updated static_analyis.db')
    return



def update_can_analyses(can_df:pd.DataFrame, *, display_updates:bool=False):
    """

    """
    # use global averages if empty can mass/volume is not available
    dyn_db = db_reg.get_instance('dynamic_analyses')
    running_metrics = dyn_db.get_data(['parameters', 'value'], 'can_measurements')

    mass_and_volumes = can_df[[i for i in can_df.columns if i.endswith('_mass') or i.endswith('_volume')]]
    
    # updates empty_can_mass with global average and create col for empty can volume
    mass_and_volumes['empty_can_mass'] = mass_and_volumes['empty_can_mass'].fillna(running_metrics.iloc[2, 1])
    mass_and_volumes.insert(len(mass_and_volumes.columns), 'empty_can_volume', mass_and_volumes['empty_can_mass'].apply(lambda x: x / 29.5)) # manual g -> fl oz conversion 


    # calculate percentage remaining mass/volume, setting the lowest possible value as 0 since using global averages results in some "negative" percentages
    pmr = ((mass_and_volumes['final_mass'] - mass_and_volumes['empty_can_mass']).apply(lambda x: max(x, 0))) / (mass_and_volumes['initial_mass'] - mass_and_volumes['empty_can_mass'])

    pvr = ((mass_and_volumes['final_volume'] - mass_and_volumes['empty_can_volume'])).apply(lambda x: max(x, 0)) / (mass_and_volumes['initial_volume'] - mass_and_volumes['empty_can_volume'])

    # objective finish status is dependent  pmr < 0.01
    ofs = pmr.apply(lambda x: True if x < OFS_THRESHOLD else False)

    package = {
        'can_id': can_df['id'],
        'objective_finish_status': ofs,
        'mass_difference': mass_and_volumes['initial_mass'] - mass_and_volumes['final_mass'],
        'true_mass_difference': (mass_and_volumes['final_mass'] - mass_and_volumes['empty_can_mass']).apply(lambda x: max(x, 0)), # difference relative to liquid content only
        'true_volume_difference': (mass_and_volumes['final_volume'] - mass_and_volumes['empty_can_volume']).apply(lambda x: max(x, 0)), # difference relative to liquid content only
        'volume_difference': mass_and_volumes['initial_volume'] - mass_and_volumes['final_volume'],
        'percentage_mass_remaining': pmr,
        'percentage_volume_remaining': pvr
    }

    ca_df = pd.DataFrame(package)
    
    if display_updates:
        print(ca_df)

    return ca_df

def update_box_analyses(
        all_raw_data:dict[str, pd.DataFrame],
        can_analyses:pd.DataFrame, 
        *, 
        display_updates:bool=False
    ):
    """
    
    """
    flavor_df = all_raw_data['box_flavors']
    
    all_packages = [] # output

    for _,row_df in flavor_df.iterrows():
        # get associated can ids based on the current flavor id to calculate can-related analyses
        # i.e. completion_percentage, average_pmr, and average_pvr
        can_ids = _get_can_ids(all_raw_data['can_data'], row_df['id'])
        analyzed_can_data = can_analyses[can_analyses['can_id'].isin(can_ids)]

        # get the purchase date from the purchaes_df at the box id of the current flavor_df row
        # convert dates to datetimes then calculate metrics
        unformat_purchase_date = all_raw_data['box_purchases'][all_raw_data['box_purchases']['id']==row_df['box_id']].to_dict(orient='records')[0]['purchase_date']
        purchase_date = datetime.datetime.strptime(unformat_purchase_date[:10], '%Y-%m-%d') if unformat_purchase_date else None

        start_date = datetime.datetime.strptime(row_df['start_date'][:10], '%Y-%m-%d') if row_df['start_date'] else None
        finish_date = datetime.datetime.strptime(row_df['finish_date'][:10], '%Y-%m-%d') if row_df['finish_date'] else None

        # metrics
        if finish_date and start_date:
            dv = finish_date - start_date
            dv_d = dv.days
        else: 
            dv_d = None
        
        if start_date and purchase_date:
            tts = start_date - purchase_date
            tts_d = tts.days
        else:
            tts_d = None

        # can related analyses
        package = {
            'box_id': row_df['id'],
            'time_to_start': tts_d,
            'drink_velocity': dv_d,
            'completion_percentage': analyzed_can_data['objective_finish_status'].sum() / len(analyzed_can_data),
            'average_pmr': analyzed_can_data['percentage_mass_remaining'].mean(),
            'average_pvr': analyzed_can_data['percentage_volume_remaining'].mean()
        }

        all_packages.append(package)

    df = pd.DataFrame(all_packages)

    if display_updates:
        print(df)

    return df


def _get_can_ids(raw_can_data:pd.DataFrame, bfid:str) -> list[str]:
    """ Gets a collection of can ids associated with the provided box (flavor) id """

    return raw_can_data[raw_can_data['box_id']==bfid]['id'].to_list()

