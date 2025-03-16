from .. import datetime, Literal, logging
from . import (read_csv_data, read_json_data, csv, os, traceback,
               SPREADSHEET_DIR, DA_DIR, DATE_FORMAT, ALT_DATE_FORMAT)

import sqlite3 as sl

logger = logging.getLogger('standard')

DBF = DA_DIR / 'database.db'
DB_TABLE_CONFIG_DATA = read_json_data(DA_DIR / 'database/table_data.json')

class Database():
    def __init__(self) -> None:
        self.db_loc = os.path.abspath(DBF)
        self.tables:list[str] = [tablename for tablename in DB_TABLE_CONFIG_DATA]
        self.box_table = 'box_data'
        self.can_table = 'can_data'
        self._base_class_parameter_amt:int = len(self.__dict__)

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

        
    def _parameter_data(self, table:str) -> list[str]:
        """ For DB inserts or user interest """
        return [specifier['name'] for specifier in DB_TABLE_CONFIG_DATA[table]]

    def _parameter_placeholders(self, parameters: list[str]) -> str:
        """ DB insert placeholders """
        placeholders = '?,' * len(parameters)
        return placeholders[:-1]
    
    def create_tables(self) -> bool:
        """ Creates tables listed in table_data.json """
        conn, curs = self.create_connection()

        def create_column_script(col_data:list[dict[str, str|bool]]) -> str:
            """ Concatenates column specifiers; only one column apart of entire execute statement """
            try:
                cols_script:list[str] = [f'{col_specifiers['name']} {col_specifiers['data type'].upper()}' for col_specifiers in col_data]
                fks:list = [f'FOREIGN KEY ({data['name']}) REFERENCES {data['foreign key'][0]}({data['foreign key'][1]})' for data in col_data if isinstance(data['foreign key'], list)]

                # PK must be first in list
                pk = [f'PRIMARY KEY ({col_data[0]['name']})']

                if len(fks) > 1:
                    return (', ').join(cols_script + pk + fks)
                return (', ').join(cols_script + pk)
            except Exception as e:
                traceback.print_exc()
                logger.error(f'Error creating column script for DB table: {col_data}')
        
        # TODO: reference table creation config variables come from CSV
        try:
            for table_name in DB_TABLE_CONFIG_DATA:
                col_script = create_column_script(DB_TABLE_CONFIG_DATA[table_name])
                curs.execute(f'CREATE TABLE IF NOT EXISTS {table_name}({col_script})')
            # curs.execute('ALTER TABLE all_slides ADD CONSTRAINT specimen_id_fk FOREIGN KEY (specimen_aperio_id) REFERENCES specimens(aperio_id)')
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



database = Database()

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

