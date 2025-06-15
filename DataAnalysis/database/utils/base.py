
from ... import datetime, Literal, logging, os, traceback, pd
from ...config import ALL_DATETIME_FORMATS, DB_DIR

from .custom_types import TablesMacroInfo, TableData

import sqlite3 as sl


logger = logging.getLogger('standard')



class Database():
    def __init__(
            self,
            database_name:str,
            *,
            table_data:TableData|None=None
    ) -> None:
        self.db_loc = os.path.join(DB_DIR, f'{database_name}.db')
        self.database_name:str = database_name

        self.table_data:TableData|None = table_data
        self.tables:TablesMacroInfo = {} 
        
        self.box_table = 'box_data'
        self.can_table = 'can_data'
        self._base_class_parameter_amt:int = len(self.__dict__)       


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

    # NOTE likely deprecate - not used
    def _parameter_placeholders(self, parameters: list[str]) -> str:
        """ DB insert placeholders """
        placeholders = '?,' * len(parameters)
        return placeholders[:-1]
    
    def view_table_info(self):
        assert self.tables, f'No table table info to view for this database: {self.database_name}'

        for table_name, data in self.tables.items():
            print(f'Table - {table_name}')
            for key, info in data.items():
                print(f'\t - {key} -> {info}')

        return
            

    
    def create_tables(self):
        """ Creates tables based on extracted data supplied to ```self.table_data``` """
        assert self.table_data is not None, 'Must supply a TableData to create tables.'
        
        conn, curs = self.create_connection()

        def create_column_script(td:pd.DataFrame) -> str:
            """ Concatenates column specifiers; only one column apart of entire execute statement """
            try:
                cols_script:list[str] = [f'{row_data['header']} {row_data['header_data_type'].upper()}' for _,row_data in td.iterrows()]

                fks = extract_foreign_key(table_data=td)

                # PK must be first in list
                pk = [f'PRIMARY KEY ({td['header'].to_list()[0]})']

                if fks:
                    return (', ').join(cols_script + pk + fks)
                return (', ').join(cols_script + pk)
            
            except Exception as e:
                traceback.print_exc()
                logger.error(f'Error creating column script for DB table: {td}')

        def extract_foreign_key(table_data:pd.DataFrame) -> list[str] | None:
            
            # extract only only properly specified/formatted foreign key values
            valid_fks:list[tuple[int, str]] = [item for item in table_data['foreign_key'].items() if ';' in item[1]]

            if len(valid_fks) == 0:
                return
            
            formatted_foreign_keys:list[str] = []
            for idx, value in valid_fks:
                ref_table, ref_column = tuple(value.split(';'))
                
                header = table_data.loc[idx]['header']
                stmt = f'FOREIGN KEY ({header}) REFERENCES {ref_table}({ref_column})'
                formatted_foreign_keys.append(stmt)

            return formatted_foreign_keys

        try:
            for table_name, table_df in self.table_data.items():
                self.tables[table_name] = {
                    'header': table_df['header'].to_list()
                }
                col_script = create_column_script(table_df)
                curs.execute(f'CREATE TABLE IF NOT EXISTS {table_name}({col_script})')
            logger.info(f'Created tables {self.tables.keys()} in {self.database_name}')
            self.close_commit(conn)
            return
        except sl.OperationalError as e:
            # traceback.print_exc()
            logger.error(f'Error creating table: {table_name}\n{col_script = }\n{e}\n')
            return

    def add_table_data(self, data:TableData, if_exist:Literal['ignore', 'overwrite']):
        """ Adds table data to instance 
        
            Args:
                data (dict) : Map of table name to table data represented as a list of dictionaries. Typically 
                              the result of db_table_config.csv processing.
                if_exists (bool) : Determines behavior if database already has table config data assigned.
                    - *ignore*: Ignores the new data in favor of the existing data. No changes are made.
                    - *overwrite*: Overwrites the existing table config data with the new data
        
        
        """
        assert isinstance(data, dict), 'Must supply dictionary containing table data'
        
        # checks whether table data already exists and behaves according to 'if_exists' parameter
        if isinstance(self.table_data, dict) and if_exist == 'ignore':
            logger.warning(f'{self.database_name} already has table config data assigned and {if_exist = }')
            return
        
        self.table_data = data
        logger.info(f'(Re)assigned table config data for {self.database_name}')

        # fill table info
        for table_name, table_df in self.table_data.items():
            self.tables[table_name] = {
                    'header': table_df['header'].to_list()
                }

        
        return

    def drop_tables(self):
        """ Drops all tables in the database """
        conn, curs = self.create_connection()
        tables = [info[0] for info in curs.execute('SELECT name FROM sqlite_master WHERE type=?', ('table',))]
        for table in tables:
            curs.execute(f'DROP TABLE {table}')
        self.close_commit(conn)
        logger.info(f'Removed all tables from {self.database_name}.db')
        return
            
    # TODO WIP
    @staticmethod
    def _generate_where_stmt(where_filter:dict[Literal['<database column>'], str|bool|int]) -> dict[Literal['stmt', 'values'], str|list[str]]:
        return {
            'stmt': '=? AND '.join([col for col in where_filter])+'=?',
            'values': [where_filter[col] for col in where_filter]
        }
        
        # TODO figure out mapping for reference retrieval
    
                    


    # NOTE likely deprecate
    def update_single_value(self, table:Literal['box_data', 'can_data'], set_data:list[Literal['<set_col>, <set_value>']], where_data:list[Literal['<where_col>, <where_value>']]):

        conn, curs = self.create_connection()
        try:
            curs.execute(f'UPDATE {table} SET {set_data[0]}=? WHERE {where_data[0]}=?', (set_data[1], where_data[1]))
        except Exception:
            logger.error(f'Error updating {table} with data: {set_data = } | {where_data = }')
        finally:
            self.close_commit(conn)

    # NOTE likely deprecate
    @staticmethod
    def standardize_date(date_input:str) -> datetime.datetime:
        if '/' in date_input:
            return datetime.datetime.strptime(date_input, '') # NOTE broken after removing DATE_FORMAT
        return datetime.datetime.strptime(date_input, ALL_DATETIME_FORMATS['ALT_DATE'])

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
