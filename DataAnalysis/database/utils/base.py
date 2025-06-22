
from ... import datetime, Literal, logging, os, traceback, pd
from ...config import ALL_DATETIME_FORMATS, DB_DIR

from .custom_types import TablesMacroInfo, TableData, WhereStatement

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
        self.tables:TablesMacroInfo = {} # NOTE maps table name to info dict with keys: header, 
        
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
            


    def get_data(self, columns:list[str], table:str, *, where_info:list[tuple[str,str]]|None=None):
        """ SQL Select data from database and returns as a dataframe
        
        """
        assert table in self.tables.keys(), f'Invalid table name {table}. Expected one from {self.tables.keys()}'

        if '*' not in columns:
            assert all(col in self.tables[table]['header'] for col in columns), f'{columns} contains an invalid column name. Valid columns defined in db_table_data.csv are: {[i for i in columns if i not in self.tables[table]['header']]}'

        if len(columns) > 1:
            stmt = f'SELECT {', '.join(columns)} FROM {table}'
        else:
            stmt = f'SELECT {columns[0]} FROM {table}'

        
        conn, _ = self.create_connection()

        # simply return if no where clause is added
        if not where_info:
            data = pd.read_sql(stmt, conn)
            # data = [i for i in curs.execute(stmt)]
        else:
            where_clause = self._generate_where_stmt(where_info)
            stmt = stmt + f' WHERE {where_clause["stmt"]}'
            data = pd.read_sql(stmt, conn, params=where_clause['values'])
            # data = [i for i in curs.execute(stmt, where_clause['values'])]

        self.close_commit(conn)
        return data

    @staticmethod
    def _generate_where_stmt(where_filter:list[tuple[str,str]]) -> WhereStatement:       
        return {
            'stmt': '=? AND '.join([info[0] for info in where_filter])+'=?',
            'values': tuple([info[1] for info in where_filter])
        }
         
    def update_values(self, table:str, set_data:dict[str,str], where_data:dict[str,str]):
        """ Updates table at the parameters provided

            Args:
                table (str) : Name of the table being updated
                set_data (dict[str,str]) : Dictionary mapping column name to value. Corresponds to syntax
                                           related to SQL SET
                where_data (dict[str,str]) : Dictionary mapping column name to value. Corresponds to syntax
                                             related to SQL WHERE
        
        """
        assert table in self.tables.keys(), f'Invalid table name {table}. Expected one from {self.tables.keys()}'

        all_cols = [col for col in set_data.keys()] + [col for col in where_data.keys()]
        assert all(col in self.table_data[table]['header'].to_list() for col in all_cols), f'Either set or where data contains invalid column name(s): {[col for col in all_cols if col not in self.table_data[table]['header']]}\n{self.table_data[table]['header'] = }'

        # updating single vs multiple values
        # two if-else modules for more flexible SQL statements
        set_stmt = f'{'=?, '.join(set_data.keys())}=?'
        where_stmt = f'{'=?, '.join(where_data.keys())}=?'
        
        # if len(set_data) > 1:
        #     set_stmt = f'{set_stmt}=?' # adds last =? since join only does in between values

        # if len(where_data) > 1:
        #     where_stmt = f'{where_stmt}=?'

        # build statement and collect parameter values
        stmt = f'UPDATE {table} SET {set_stmt} WHERE {where_stmt}'
        all_vals = [val for val in set_data.values()] + [val for val in where_data.values()]

        try:
            conn, curs = self.create_connection()
            curs.execute(stmt, tuple(all_vals))
            self.close_commit(conn)
        except Exception as e:
            print(f'{stmt = }')
            raise e

        logger.info(f'Updated {table} at {set_data.keys()}')

        return

    def display_data(self):
        class_data = {param:self.__dict__[param] for param in self.__dict__ if param in self.CLASS_PARAMS}
        for data in class_data:
            print(f'{data}: {class_data[data]}')
    
