from .. import Literal, logging, pd, pathlib, generate, TypeAlias, os
from ..config import DB_CONFIG_DIR,DB_DIR, EXTERNAL_DATA_DIR

from .registry import Database, DatabaseRegistry
from .processor import DataProcessor

logger = logging.getLogger('standard')
db_reg = DatabaseRegistry(base_database_dir=DB_DIR)



TableData:TypeAlias = dict[str, pd.DataFrame]
TableMap:TypeAlias = dict[Literal['table_order_map'], TableData]
DatabaseConfigMap:TypeAlias = dict[str, TableMap]



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

def process_and_export_lc_data(
        base_data_dir:str, 
        filter_export_collections:tuple[str]=('*',),
        *, 
        display_processing_stats:bool=False,
        db_export:bool=False, 
        database_registry:DatabaseRegistry|None=None, 
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
        assert database_registry, f'Must supply a DatabaseRegistry to export to database'
        raw_data_db = database_registry.get_instance('raw_data')
        
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
    
def create_reset_databases(database_registry:DatabaseRegistry, reset:bool=False):
    """ Creates or resets database(s) based on config file then registers them to the global registry
    
        Args:
            database_registry (DatabaseRegistry) : Master collection of created and registered databases
            reset (bool) : If true, will delete and recreate all registered databases. Default is False.
    
    
    """
    if reset:
        database_registry.reset_databases()

    # recreate tables per current DB TABLE CONFIG file TODO check whether  
    db_config_data_file = DB_CONFIG_DIR / 'db_table_data.csv'
    formatted_db_config = _format_db_config(db_config_data_file)

    temp = []

    for database_name,table_data in formatted_db_config.items():
        if database_registry.validate_registration(database_name):
            continue

        db_table_data = _process_table_data(table_data)
        temp.append(db_table_data)
        dbo = Database(database_name=database_name, table_data=db_table_data) # TODO continue tracking table_data, and ensure Database init can properly handle DatabaseConfigMap type  
        dbo.create_tables()

        database_registry.add_instance(dbo)

    return temp

def _format_db_config(path_to_config_file:pathlib.Path) -> DatabaseConfigMap:
    """ Extracts config parameters from a CSV file as a dataframe, then Restructures table config data 
        preserving the original order of database name, table name, and header 
        
        Output dictionary hierarchy:
            ```
                {
                    <database name>: {
                        'table_order_map': {
                            <table name>: <table dataframe>
                        }
                    }
                }
            ```

        For example:
           ```
            {
                master: {
                    'table_order_map': {
                        reference: Dataframe(columns=header, header_data_type, foreign_key),
                        ...
                    }
                },
                ...
            }
        ``` 

        Call order: 
            1. Database name
            2. "table_order_map"
            3. Table name
        To return dataframe of config parameters for that table.
        
    
    """
    data = pd.read_csv(path_to_config_file)
    
    # retain the original order of database names as in the CSV
    # final form will be an ordered db config map with preserved database name,
    # table name, and header (in that priority as well) as ordered in the CSV
    db_order = {i:{} for i in data['database_name'].to_list()}

    # groupby() changes the row order from the CSV -> 'dbdf' is unsorted table data 
    # order matters for valid table creation, so we resort it according to the original CSV 'data' (pd.Dataframe)
    for databasename,dbdf in data.groupby('database_name'):
        db_order[databasename]['tables_unordered'] = dbdf
        tables_in_this_db = data[data['database_name'] == databasename]['table_name']

        db_order[databasename]['table_order_map'] = {i:{} for i in tables_in_this_db.to_list()}
    

    for _,tables_dict in db_order.items():
        for tablename,tbdf in tables_dict['tables_unordered'].groupby('table_name'):
            tables_dict['table_order_map'][tablename] = tbdf
        tables_dict.pop('tables_unordered')
    
    return db_order

def _process_table_data(table_data_map:TableMap) -> TableData:
    """ Prepares dict for  """

    all_tables = {}
    
    for table_name, table_df in table_data_map['table_order_map'].items(): # NOTE .items() result is type TableData
        # print(f'{table_name}\n\n\t{table_name}\n\t\t{table_df}\n')

        # Extract only columns pertaining to table data
        table_config_only = table_df[['header', 'header_data_type', 'foreign_key']]

        # Format foreign_key to list if specified 
        table_config_only['foreign_key'].apply(lambda x: x.split(';') if ';' in x else False)
        
        # tbl_cnfg_dct:list[dict[str,str]] = table_config_only.to_dict(orient='records') # type:ignore

        # NOTE version with tbl_confg kept as a DF

        all_tables[table_name] = table_config_only 
    
    return all_tables



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
   



database = None

def get_table_property(table: Literal['<database table>'], property:Literal['<database column>'], where_row:Literal['<database column>']=None, where_value:str|bool|int=None, count:bool=False):
    """Search for property from all specimens or specify a specific row"""
    conn, curs = database.create_connection()

    if where_row:
        data = [info[0] for info in curs.execute(f'SELECT {property} FROM {table} WHERE {where_row}=?', (where_value,))]
    else:
        data = [info[0] for info in curs.execute(f'SELECT {property} FROM {table}')]
    
    database.close_commit(conn)
    
    if count:
        return len(data)
    return data

def get_multiple_table_properties(table: Literal['<database table>'], count:bool=False, *properties:list[str], **where_specifiers) -> list[tuple] | int:
    conn, curs = database.create_connection()

    data = [info for info in curs.execute(f'SELECT {','.join(properties)} FROM {table}')]
    if where_specifiers:
        where_data = database._generate_where_stmt(where_specifiers)

        data = [info for info in curs.execute(f'SELECT {','.join(properties)} FROM {table} WHERE {where_data['stmt']}', tuple(where_data['values']))]

    database.close_commit(conn)

    if count:
        return len(data)
    return data


def check_abbreviation_existence(abbreviation:str) -> bool:
    """Validates whether specified abbreviation exists in reference database"""
    if abbreviation in get_table_property('reference_data', 'abbreviation'):
        return True
    return False




# Quick Views 
def get_all_boxes() -> list[str]:
    """Returns list of box IDs"""

