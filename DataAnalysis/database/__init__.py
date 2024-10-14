"""
Directory to interface with the Django database 
"""
import sqlite3 as sl
from .. import os, logger, traceback, read_json_data, DA_DIR, Literal, datetime, DATE_FORMAT

# from LCTSite.app.models import BoxTracker, CanData

DBF = DA_DIR / 'database.db'

DB_TABLE_CONFIG_DATA = read_json_data(DA_DIR / 'database/table_data.json')

class Database():
    def __init__(self) -> None:
        self.db_loc = os.path.abspath(DBF)
        self.box_table = 'box_data'
        self.can_table = 'can_data'
        self._base_class_parameter_amt:int = len(self.__dict__)

    def _create_connection(self) -> tuple[sl.Connection, sl.Cursor]:
        """ Creates/Returns a connection and cursor """
        conn = sl.connect(self.db_loc, check_same_thread=False)
        curs = conn.cursor()
        return conn, curs
    
    def _close_commit(self, connection:sl.Connection) -> None:
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
        """ Creates Specimen and Slide tables """
        conn, curs = self._create_connection()

        def create_column_script(col_data:list[dict[str, str|bool]]) -> str:
            """ Concatenates column specifiers; only one column apart of entire execute statement """
            try:
                cols_script:list[str] = [f'{col_specifiers['name']} {col_specifiers['data type'].upper()}' for col_specifiers in col_data]
                fks:list[str] = [col_specifiers['foreign key'] for col_specifiers in col_data if isinstance(col_specifiers['foreign key'], str)]

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
            self._close_commit(conn)
            return True
        except sl.OperationalError as e:
            # traceback.print_exc()
            logger.error(f'Error creating table: {table_name}\n{col_script = }\n{e}\n')
            return False

    def _recreate_tables(self):
        conn, curs = self._create_connection()
        tables = [info[0] for info in curs.execute('SELECT name FROM sqlite_master WHERE type=?', ('table',))]
        for table in tables:
            curs.execute(f'DROP TABLE {table}')
        self._close_commit(conn)
        return self.create_tables()
    
        
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

        conn, curs = self._create_connection()
        try:
            curs.execute(f'UPDATE {table} SET {set_data[0]}=? WHERE {where_data[0]}=?', (set_data[1], where_data[1]))
        except Exception:
            logger.error(f'Error updating {table} with data: {set_data = } | {where_data = }')
        finally:
            self._close_commit(conn)


    def add_box(self, insert_data:tuple):
        conn, curs = self._create_connection()
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
            self._close_commit(conn)

    def add_can(self, insert_data:tuple):
        conn, curs = self._create_connection()
        
        params:list = self._parameter_data(self.can_table)
        try:
            curs.execute(f'INSERT INTO {self.can_table}({", ".join(params)}) VALUES({self._parameter_placeholders(params)})', insert_data)
            return
        except Exception as e:
            logger.error(f'error inserting can data: {insert_data = }\n{e}')
        finally:
            self._close_commit(conn)


    def _date_time_difference(self, final_date:str, initial_date:str) -> datetime.timedelta:
        final = datetime.datetime.strptime(final_date, DATE_FORMAT)
        initial = datetime.datetime.strptime(initial_date, DATE_FORMAT)
        return final - initial 
    
    def display_data(self):
        class_data = {param:self.__dict__[param] for param in self.__dict__ if param in self.CLASS_PARAMS}
        for data in class_data:
            print(f'{data}: {class_data[data]}')
