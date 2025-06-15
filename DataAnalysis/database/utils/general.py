
from ... import logging, os, pd
from ...utils import get_current_time, PickleHandler
from ...config import DB_CONFIG_DIR

from ..utils.registry import Database, DatabaseRegistry

from .custom_types import DatabaseConfigMap, TableData, TableMap






logger = logging.getLogger('standard')
db_reg = DatabaseRegistry()
pickler = PickleHandler()


db_cdf = DB_CONFIG_DIR / 'db_table_data.csv'


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

def format_db_config(path_to_config_file:str) -> DatabaseConfigMap:
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
    assert path_to_config_file.endswith('.csv'), f'Must supply a path to a CSV file, not: {path_to_config_file}'
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

def process_table_data(table_data_map:TableMap) -> TableData:
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


    
