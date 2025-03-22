from .. import datetime, Literal, logging, csv, os, traceback, pd
from ..config import SPREADSHEET_DIR, DATE_FORMAT, ALT_DATE_FORMAT, DB_CONFIG_DIR, DB_DIR
from ..utils import read_csv_data, read_json_data

import sqlite3 as sl

logger = logging.getLogger('standard')







DB_TABLE_CONFIG_DATA = {}


class Database():
    def __init__(
            self,
            database_name:str,
            database_registry,
            *,
            table_data:list[dict[str,str]]=None
    ) -> None:
        self.db_loc = os.path.join(DB_DIR, f'{database_name}.db')
        self.database_name = database_name
        self.table_data:dict[str, list[dict[str,str]]] = table_data
        self.tables:dict[str,str|list[str]] = {}
        
        self.box_table = 'box_data'
        self.can_table = 'can_data'
        self._base_class_parameter_amt:int = len(self.__dict__)       

        database_registry.add_instance(self)

    def __repr__(self):
        return f'{self.database_name} Database'

    def create_connection(self) -> tuple[sl.Connection, sl.Cursor]:
        """ Creates/Returns a connection and cursor """
        conn = sl.connect(self.db_loc, check_same_thread=False)
        curs = conn.cursor()
        return conn, curs
    
    def close_commit(self, connection:sl.Connection) -> None:
        """ Commits changes then closes connection"""
        connection.commit()
        connection.close()
        return

    
    # TODO essentially replaced with self.tables[<table_name>]['headers']
    def _parameter_data(self, table_name:str) -> list[str]:
        """ For DB inserts or user interest """
        return [specifier['name'] for specifier in self.tables[table_name]]

    def _parameter_placeholders(self, parameters: list[str]) -> str:
        """ DB insert placeholders """
        placeholders = '?,' * len(parameters)
        return placeholders[:-1]
    
    def add_table_data(self, data:dict[str, list[dict[str,str]]]):
        """ Adds table data to instance """
        assert isinstance(data, dict), 'Must supply dictionary containing table data'
        for tblnm in data:
            assert isinstance(data[tblnm], list), f'Dictionary must contain list values, not {type(data[tblnm])}'
        
        self.table_data = data
        return
        

    
    def create_tables(self) -> bool:
        """ Creates tables listed in table_data.json """
        assert isinstance(self.table_data, dict), 'Must supply table data to create tables'
        
        conn, curs = self.create_connection()

        def create_column_script(col_data:list[dict[str, str|bool]]) -> str:
            """ Concatenates column specifiers; only one column apart of entire execute statement """
            try:
                cols_script:list[str] = [f'{col_specifiers['header']} {col_specifiers['header_data_type'].upper()}' for col_specifiers in col_data]
                fks:list = [f'FOREIGN KEY ({data['header']}) REFERENCES {data['foreign_key'][0]}({data['foreign_key'][1]})' for data in col_data if isinstance(data['foreign_key'], list)]

                # PK must be first in list
                pk = [f'PRIMARY KEY ({col_data[0]['header']})']

                if len(fks) > 0:
                    return (', ').join(cols_script + pk + fks)
                return (', ').join(cols_script + pk)
            except Exception as e:
                traceback.print_exc()
                logger.error(f'Error creating column script for DB table: {col_data}')

        try:
            for table_name in self.table_data:
                self.tables[table_name] = {'headers':[i['header'] for i in self.table_data[table_name]]}
                col_script = create_column_script(self.table_data[table_name])
                curs.execute(f'CREATE TABLE IF NOT EXISTS {table_name}({col_script})')
            # curs.execute('ALTER TABLE all_slides ADD CONSTRAINT specimen_id_fk FOREIGN KEY (specimen_aperio_id) REFERENCES specimens(aperio_id)')
            logger.info(f'Created tables {self.tables.keys()}')
            self.close_commit(conn)
            return True
        except sl.OperationalError as e:
            # traceback.print_exc()
            logger.error(f'Error creating table: {table_name}\n{col_script = }\n{e}\n')
            return False

    def _recreate_tables(self):
        conn, curs = self.create_connection()
        tables = [info[0] for info in curs.execute('SELECT name FROM sqlite_master WHERE type=?', ('table',))]
        for table in tables:
            curs.execute(f'DROP TABLE {table}')
        self.close_commit(conn)
        self.create_tables()
        logger.info(f'Recreated Database tables: {self.tables}')
        return
            
    @staticmethod
    def _generate_where_stmt(where_filter:dict[Literal['<database column>'], str|bool|int]) -> dict[Literal['stmt', 'values'], str|list[str]]:
        return {
            'stmt': '=? AND '.join([col for col in where_filter])+'=?',
            'values': [where_filter[col] for col in where_filter]
        }
        
        # TODO figure out mapping for reference retrieval
    

    def db_insert(self) -> tuple[str | bool | int]:
        """ formatted properties to insert into DB """
        # return f'{self._base_class_parameter_amt = }'
        package:list[str | bool | int] = [self.__dict__[parameter] for parameter in self.__dict__][self._base_class_parameter_amt+2:]
        return tuple(package) 

    def update_single_value(self, table:Literal['box_data', 'can_data'], set_data:list[Literal['<set_col>, <set_value>']], where_data:list[Literal['<where_col>, <where_value>']]):

        conn, curs = self.create_connection()
        try:
            curs.execute(f'UPDATE {table} SET {set_data[0]}=? WHERE {where_data[0]}=?', (set_data[1], where_data[1]))
        except Exception:
            logger.error(f'Error updating {table} with data: {set_data = } | {where_data = }')
        finally:
            self.close_commit(conn)


    def add_box(self, insert_data:tuple):
        conn, curs = self.create_connection()
        existing_specs = [info[0] for info in curs.execute('SELECT box_id FROM box_data')]

        try:
            if insert_data[0] not in existing_specs:
                params:list[str] = self._parameter_data(self.box_table)
                curs.execute(f'INSERT INTO {self.box_table}({', '.join(params)}) VALUES({self._parameter_placeholders(params)})', insert_data)
                return
            return
        except Exception as e:
            traceback.print_exc()
            logger.error(f'error inserting specimen data: {insert_data = }')
        finally:
            self.close_commit(conn)

    def add_can(self, insert_data:tuple):
        conn, curs = self.create_connection()
        
        params:list = self._parameter_data(self.can_table)
        try:
            curs.execute(f'INSERT INTO {self.can_table}({", ".join(params)}) VALUES({self._parameter_placeholders(params)})', insert_data)
            return
        except Exception as e:
            logger.error(f'error inserting can data: {insert_data = }\n{e}')
        finally:
            self.close_commit(conn)


    @staticmethod
    def standardize_date(date_input:str) -> datetime.datetime:
        if '/' in date_input:
            return datetime.datetime.strptime(date_input, DATE_FORMAT)
        return datetime.datetime.strptime(date_input, ALT_DATE_FORMAT)

    def _date_time_difference(self, final_date:str, initial_date:str) -> datetime.timedelta:
        final = self.standardize_date(final_date)
        initial = self.standardize_date(initial_date)
        return final - initial 
    
    def display_data(self):
        class_data = {param:self.__dict__[param] for param in self.__dict__ if param in self.CLASS_PARAMS}
        for data in class_data:
            print(f'{data}: {class_data[data]}')
    
    @staticmethod
    def is_value_empty(value) -> bool:
        if value in ['nan', '', 'NA']:
            return True
        return False



class DatabaseRegistry:
    """ Global registry where database instances are stored """
    _instances:dict[str,Database] = {}

    @classmethod
    def add_instance(cls, dbi:Database):
        """Retrieve an existing database instance or create a new one."""

        if dbi.database_name not in cls._instances:
            cls._instances[dbi.database_name] = dbi
            logger.info(f'Added new database ({dbi.database_name}) to global registry')
            return
        logger.warning(f'Database ({dbi.database_name}) instance already exists in the global registry')

        return
    
    @classmethod
    def get_instance(cls, db_name:str) -> Database:
        assert db_name in cls._instances.keys(), f'Database Name ({db_name}) doesnt exist in the global registry'
        return cls._instances[db_name]

    @classmethod
    def update_instance(cls, dbi:Database):
        """ Updates existing instance with new instance at the database name, or adds new instance to registry """
        if dbi.database_name in cls._instances:
            cls._instances[dbi.database_name] = dbi
            logger.info(f'Updated database {dbi.database_name} instance in the global registry')
        else:
            cls._instances[dbi.database_name] = dbi
            logger.info(f'Database Name ({dbi.database_name}) was not found in the global registry, but was added')
        return
    
    @classmethod
    def remove_instance(cls, db_name:str):
        """ Removes a database instance from the global registry by database name """
        assert db_name in cls._instances, f'Database Name ({db_name}) doesnt exist in the registry'
        cls._instances.pop(db_name)
        logger.info(f'Removed database {db_name} from the global registry')
        return
    
    @classmethod
    def get_all_instances(cls) -> dict[str,Database]:
        return cls._instances
    
    @classmethod
    def _reset_all_db_tables(cls):
        for dbn, dbi in cls._instances.items():
            dbi._recreate_tables()
            logger.info(f'Reset tables in {dbn}.db')
        return


dbr = DatabaseRegistry()

def process_db_config():
    """ Processes the database config data in the config_sheets directory """
    db_config_data_file = DB_CONFIG_DIR / 'db_data.csv'
    db_config_data_df = pd.read_csv(db_config_data_file)
    formatted_db_config = _format_db_config(db_config_data_df)

    for database_name,table_data in formatted_db_config.items():
        db_table_data = _process_table_data(table_data)
        dbo = Database(database_name=database_name, database_registry=dbr, table_data=db_table_data)
        dbo.create_tables()


    return 

def _process_table_data(table_data:dict[str, pd.DataFrame], print_formatted_config:bool = False):
    """ DB Config helper function that handles table data extraction """

    all_tables = {}
    
    for table_name, table_df in table_data['table_order_map'].items():
        if print_formatted_config:
            print(f'{table_name}\n\n\t{table_name}\n\t\t{table_df}\n')

        # removes database and table name from DF
        table_config_only = table_df[['header', 'header_data_type', 'foreign_key']]
        
        tbl_cnfg_dct = table_config_only.to_dict(orient='records')
        tb_cfg_dct_formatted_fk = _extract_foreign_key(tbl_cnfg_dct) # same as above but with FK formatted as bool or list
        all_tables[table_name] = tb_cfg_dct_formatted_fk
    
    return all_tables

def _extract_foreign_key(config_data:list[dict[str,str]]):
    for info in config_data:
        if ';' in info['foreign_key']:
            value = info['foreign_key'].split(';')
        else: 
            value = False
        config_data[config_data.index(info)]['foreign_key'] = value
    return config_data

def _format_db_config(data:pd.DataFrame) -> dict[str, dict[str,dict[str,pd.DataFrame]]]:
    """ Restructures table config data preserving the original order of database name, table name, and header """
    # get ordered database name in correct creation order 
    # final form will be an ordered db config map with preserved database name,
    # table name, and header (in that priority as well) as ordered in the CSV
    db_order = {i:{} for i in data['database_name'].to_list()}

    for databasename,dbdf in data.groupby('database_name'):
        db_order[databasename]['tables_unordered'] = dbdf
        db_order[databasename]['table_order_map'] = _sort_grouping_per_csv(data.loc[data['database_name'] == databasename]['table_name'])
    
    for _,tables_dict in db_order.items():
        for tablename,tbdf in tables_dict['tables_unordered'].groupby('table_name'):
            tables_dict['table_order_map'][tablename] = tbdf
        tables_dict.pop('tables_unordered')
    
    return db_order


def _sort_grouping_per_csv(data:pd.Series) -> dict[str,None]:
    """ Extracts unique values from a pd.Series while retaining its original order as presented in the CSV """
    return {i:{} for i in data.to_list()}




class Can(Database): 
    def __init__(self, can_id:str, initial_data:list[str]=None):
        super().__init__()
        self.can_id:str = can_id
        self.initial_mass:int = 0
        self.initial_volume:float = 0.0
        self.final_mass:int = 0
        self.final_volume:float = 0.0
        self.complete_status:str = ''
        self.percent_remaining_mass:float = 0
        self.percent_remaining_volume:float = 0

        if initial_data:
            s_bid = initial_data['Box']
            self.can_id = f'{initial_data['Can']}.{s_bid}'
            self.initial_mass:float = float(initial_data['Initial Mass']) if not self.is_value_empty(initial_data['Initial Mass']) else 'NA'
            self.initial_volume:float = float(initial_data['Initial Volume']) if not self.is_value_empty(initial_data['Initial Volume']) else 'NA'
            self.final_mass:float = float(initial_data['Final Mass']) if not self.is_value_empty(initial_data['Final Mass']) else 'NA'
            self.final_volume:float = float(initial_data['Final Volume']) if not self.is_value_empty(initial_data['Final Volume']) else 'NA'
            self.complete_status:str = initial_data['Finished'] if not self.is_value_empty(initial_data['Finished']) else 'NA'

            self.percent_remaining_mass:float|str = self.calculate_percent_remaining_mass()
            self.percent_remaining_volume:float|str = self.calculate_percent_remaining_volume()
        

        self.CLASS_PARAMS = [name for name in self.__dict__][self._base_class_parameter_amt+1:]
    
    def calculate_percent_remaining_mass(self):
        if isinstance(self.initial_mass, str) or isinstance(self.final_mass, str):
            return 'NA'
        return round((self.final_mass / self.initial_mass)*100, 3)
    
    def calculate_percent_remaining_volume(self):
        if isinstance(self.initial_volume, str) or isinstance(self.final_volume, str):
            return 'NA'
        return round((self.final_volume / self.initial_volume)*100, 3)

    def db_insert(self):
        package = [self.__dict__[parameter] for parameter in self.__dict__][self._base_class_parameter_amt+1:-1]
        self.add_can(tuple(package))

    def _retrieve_data(self):
        """Fills object with existing data from DB"""
        conn, curs = self.create_connection()
        can_data = [info for info in curs.execute(f'SELECT * FROM {self.can_table} WHERE can_id=?', (self.can_id,))][0]
        self.close_commit(conn)

        for data_parameter, value in zip(self.CLASS_PARAMS, can_data):
            self.__dict__[data_parameter] = value
        
        self.fill_can_data()

        return
    

class Box(Database):
    def __init__(self, box_id:str, initial_data:list[str]=None):
        super().__init__()
        self.box_id:str = box_id
        self.flavor:str = ''
        self.purchase_date:str = ''
        self.price:float = 0.00
        self.location:str = ''
        self.start_date:str = ''
        self.finish_date:str = ''
        self.drink_velocity:int = 0
        self.time_to_start:int = 0

        if initial_data:

            self.flavor:str = initial_data['flavor'] 
            self.purchase_date:str = initial_data['purchase_date']
            self.price:float|str = float(initial_data['price']) if not self.is_value_empty(initial_data['price']) else 'NA'
            self.location:str = initial_data['location'] if not self.is_value_empty(initial_data['location']) else 'NA'
            self.start_date:str = initial_data['started'] if not self.is_value_empty(initial_data['started']) else 'NA'
            self.finish_date:str = initial_data['finished'] if not self.is_value_empty(initial_data['finished']) else 'NA'
            # replace these with functions to calculate them in python, rather than taking from excel
            # can only caluclate if dates aren't NA
            self.drink_velocity:int|str = self.calculate_drink_velocity()
            self.time_to_start:int|str = self.calculate_time_to_start()

    
        self.cans:list[Can] = self.fill_can_data()

        self.CLASS_PARAMS = [name for name in self.__dict__][self._base_class_parameter_amt+1:]

    def calculate_drink_velocity(self) -> int:
        if self.start_date == 'NA' or self.finish_date == 'NA':
            return 'NA'
        dv:datetime.timedelta = self._date_time_difference(self.finish_date, self.start_date)
        return dv.days
    
    def calculate_time_to_start(self) -> int:
        if self.purchase_date == 'NA' or self.start_date == 'NA':
            return 'NA'
        tts:datetime.timedelta = self._date_time_difference(self.start_date, self.purchase_date)
        return tts.days

    def fill_can_data(self):
        """Grabs associated can data from DB for reference upon box data retrieval"""
        conn, curs = self.create_connection()
        associated_cans = [info[0] for info in curs.execute('SELECT can_id FROM can_data WHERE can_id LIKE ?', (f'%{self.box_id}%',))]
        self.close_commit(conn)
        return associated_cans
    

        

    def db_insert(self):
        package = [self.__dict__[parameter] for parameter in self.__dict__][self._base_class_parameter_amt+1:-2]
        self.add_box(tuple(package))

    def _retrieve_data(self):
        """Fills object with existing data from DB"""
        conn, curs = self.create_connection()
        box_data = [info for info in curs.execute(f'SELECT * FROM {self.box_table} WHERE box_id=?', (self.box_id,))][0]
        self.close_commit(conn)

        for data_parameter, value in zip(self.CLASS_PARAMS[:-1], box_data):
            self.__dict__[data_parameter] = value
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




def map_to_can(box_box_id:str) -> str | bool:
    """Maps prior box box id to prior can box id"""
    with open(os.path.join(SPREADSHEET_DIR, 'boxid_map.csv'), 'r') as fn:
        hdr = fn.readline().strip().split(',')
        BOX_ID_MAPPER:list[dict[Literal['box_box_id', 'can_box_id'],str]] = [info for info in csv.DictReader(fn, hdr)]
        # print(f'BIMR hdr: {hdr}\n{BOX_ID_MAPPER = }')

    formatted_data = {dicts['can_box_bid']:dicts['box_box_id'] for dicts in BOX_ID_MAPPER}
    logger.debug(f'{ formatted_data = }')
    if box_box_id in formatted_data:
        return formatted_data[box_box_id]
    logger.warning(f'{box_box_id} does not exist in map.') 
    return False

