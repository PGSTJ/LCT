from .. import Literal, logging, pd, os

from .custom_types import DatabaseConfigMap, TableData, TableMap


logger = logging.getLogger('standard')




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


def check_for_saved_table_data(output_dir_path:str) -> None|str:
    """ Checks for any pickled table data from previous extractions in the provided directory.
    
        Args:
            output_dir_path (str) : Path to the directory being search for saved table data

        Returns:
            A path to the saved data, if found
    
    """

    assert os.path.isdir(output_dir_path), f'Must supply a path to a directory, not: {output_dir_path}'

    found_saves = [fn for fn in os.listdir(output_dir_path) if fn.endswith('_table_data.pkl')]
    total_saves = len(found_saves)

    if total_saves == 0:
        logger.warning(f'No pickle saves identified in the provided directory: {output_dir_path}')
        return None

    elif total_saves == 1:
        logger.info(f'Loading save from {found_saves[0]}')
        fp = os.path.join(output_dir_path, found_saves[0])
        return fp
    
    logger.info(f'Multiple saves detected: {found_saves}')
    print(f'Multiple saves detected ({total_saves}). Choose from one below:')

    assertion = False
    while not assertion: 

        for save in found_saves:
            print(f'- {save}')
        
        option = input('> ')

        assert option in found_saves, f'Invalid option: {option}. Must choose from below:'

        option_idx = found_saves.index(option)
        assertion = True

    fp = os.path.join(output_dir_path, found_saves[option_idx])
    return fp

    



def _calculate_box_analyses():
    """
    
    """







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

