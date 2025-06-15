
from .. import Literal, logging, pd, generate, os
from ..config import DB_DIR, DB_CONFIG_DIR, EXTERNAL_DATA_DIR
from ..utils import PickleHandler, get_current_time

from .registry import Database, DatabaseRegistry



from .registry import Database, DatabaseRegistry
from .processor import DataProcessor
from .utils import format_db_config, process_table_data


logger = logging.getLogger('standard')
db_reg = DatabaseRegistry(base_database_dir=DB_DIR)
pickler = PickleHandler()


db_cdf = DB_CONFIG_DIR / 'db_table_data.csv'
rdf = DB_CONFIG_DIR / 'reference_data.csv'



def upload_reference_data(database_registry:DatabaseRegistry, reference_data_file_path:str=str(rdf)):
    """Creates reference table of abbreviations based on initial data.
    
    Data specific reference types include: Flavors, Location. Additional types 
    include: Development, Administrative, Exceptions
    
    """
    assert reference_data_file_path.endswith('.csv'), f'Must supply a path to a CSV file, not : {reference_data_file_path}'
    reference_df = pd.read_csv(reference_data_file_path)

    ids = [generate(size=7) for _ in range(len(reference_df))]
    reference_df.insert(0, 'id', ids)


    db = database_registry.get_instance('master')
    conn, _ = db.create_connection()

    reference_df.to_sql('reference', con=conn, if_exists='replace', index=False)
    db.close_commit(conn)
    logger.info(f'Created reference table in the master database')
    return

def process_and_export_lc_data(
        base_data_dir:str, 
        filter_export_collections:tuple[str]=('*',),
        *, 
        display_processing_stats:bool=False,
        db_export:bool=False, 
        db_override:bool = False,
        file_export:Literal['csv', 'pickle']|None=None,
        output_dir_map:dict[str, str]|None=None,
        output_file_map:dict[str, str]|None=None,
    ):
    """ Processes all raw data in La Croix Data directory and exports to a database or specified file

        Args:
            base_data_dir (str) : Path to the directory containing raw CSV and MD data
            filter_export_collections (tuple[str]) : Optionally filter for specific collections to export.
                                                     Default is "*", which exports all collections. 
                                                                                            
            display_processing_stats (bool) : If true, prints various stats about the processing run. Default 
                                              is False.
            db_export (bool) : If true, exports newly processed data to the database (currently 'raw_data.db'). 
                               Default is False. Must supply a DatabaseRegistry if True.
            database_registry (DatabaseRegistry) : Master collection of created and registered databases. Only
                                                   required if db_export=True
            db_override (bool) : If true, will override any existing contents in the tables. Default is False.
            file_export (Literal['csv', 'pickle']) : Specifies file type to export newly processed data to.
            output_dir_map (dict[str, str]) : Override default output directory locations. Only required if
                                              all=False AND output_dir_map is None.                            
            output_file_map (dict[str, str]) : Override default output file names.
    
    """
    assert os.path.exists(base_data_dir), f'Base data dir path does not exist: {base_data_dir}'
    assert os.path.isdir(base_data_dir), f'Must supply a path to a directory, not: {base_data_dir}'

    # NOTE cannnn generalize to '_raw' presence, but being explicit is probably better
    req_subdirs = ['csv_raw', 'md_raw']
    valid_subdirs = [i for i in os.listdir(base_data_dir) if i in req_subdirs]
    assert len(valid_subdirs) == 2, f'Base data dir does not contain the required sub-directories: csv_raw or md_raw'

    csv_data, md_data = tuple([os.path.join(base_data_dir, rsd) for rsd in req_subdirs])

    procesor = DataProcessor()
    procesor.run_pre_processing(csv_data_dir=csv_data, md_data_dir=md_data)

    if display_processing_stats:
        procesor.display_run_stats()

    if db_export:
        assert db_reg, f'Must supply a DatabaseRegistry to export to database'
        raw_data_db = db_reg.get_instance('raw_data')
        
        procesor.db_export(raw_data_db, filter_export_collections, override=db_override)

        logger.info('Finished exporting to database')
    
    if file_export:
        all_data = True if filter_export_collections == '*' else False
        procesor.file_export(
            type=file_export,
            all_data=all_data,
            collections=filter_export_collections,
            output_dir_map=output_dir_map,
            output_file_map=output_file_map
        )

        logger.info(f'Finished exporting data to {file_export}')

    return
    
def create_reset_databases(
        db_config_file_path:str=str(db_cdf), 
        reset:bool=False, 
        save_table_data:bool=False,
        output_dir_path:str|None=None
    ):
    """ Creates or resets database(s) based on config file then registers them to the global registry
    
        Args:
            database_registry (DatabaseRegistry) : Master collection of created and registered databases
            db_config_file_path (str) : Path to the database config CSV file
            reset (bool) : If true, will delete and recreate all registered databases. Default is False.
            save_table_date (bool) : If true, will pickle save extracted table data to the specified output_path.
                                     Default is False.
            output_path (str) : Path to the directory containing saved table data
    
    
    """
    if reset:
        db_reg.reset_databases()

    # recreate tables per current DB TABLE CONFIG file TODO check whether  
    formatted_db_config = format_db_config(db_config_file_path)

    all_table_data = {}

    for database_name,table_data in formatted_db_config.items():
        if db_reg.validate_registration(database_name):
            continue

        db_table_data = process_table_data(table_data)
        all_table_data[database_name] = db_table_data

        dbo = Database(database_name=database_name, table_data=db_table_data) 
        dbo.create_tables()

        db_reg.add_instance(dbo)

    if save_table_data:
        assert output_dir_path, f'Must supply output destination to save table data'
        assert os.path.isdir(output_dir_path), f'Must supply path to a directory, not: {output_dir_path}'

        ct = get_current_time(format='FILE_DATE')
        fp = os.path.join(output_dir_path, f'{ct}_table_data.pkl')

        pickler.save_pickle(all_table_data, fp)
        print(f'Pickled table data to {fp}')

    return



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











class DAUtilPipelinePresets:
    """ Various preserved states of common run sequences """
    _total_presets:int = 0

    @classmethod
    def run_preset(cls, preset:int):
        if preset == 1:
            cls.preset_one()
        
        else:
            raise ValueError(f'Invalid preset {preset}. Choose between 1 and {cls._total_presets}')
        
        return
                

    @staticmethod
    def preset_one():
        """ Resets Databases, processes raw data and uploads to database 
        
        
        
        """
        print(f'Preset 1 - Resets Databases, processes raw data and uploads to database')

        create_reset_databases(database_registry=db_reg, reset=True)
        upload_reference_data(db_reg)
        process_and_export_lc_data(
            base_data_dir=str(EXTERNAL_DATA_DIR),
            display_processing_stats=True,
            db_export=True,
            database_registry=db_reg
        )
        return
   