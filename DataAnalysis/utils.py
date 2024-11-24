from . import ALL_BOX_DATA_DF, ALL_CAN_DATA_DF, DATE_FORMAT, pd, np, datetime, Literal
from .database import Database

database = Database()

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
            self.can_id = f'{initial_data['Can']}.{initial_data['Box']}'
            self.initial_mass:int = int(initial_data['Initial Mass']) if initial_data['Initial Mass'] != 'nan' else 'NA'
            self.initial_volume:float = float(initial_data['Initial Volume']) if initial_data['Initial Volume'] != 'nan' else 'NA'
            self.final_mass:int = int(initial_data['Final Mass']) if initial_data['Final Mass'] != 'nan' else 'NA'
            self.final_volume:float = float(initial_data['Final Volume']) if initial_data['Final Volume'] != 'nan' else 'NA'
            self.complete_status:str = initial_data['Finished'] if initial_data['Finished'] != 'nan' else 'NA'

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
        conn, curs = self._create_connection()
        can_data = [info for info in curs.execute(f'SELECT * FROM {self.can_table} WHERE can_id=?', (self.can_id,))][0]
        self._close_commit(conn)

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
            self.price:float|str = float(initial_data['price']) if initial_data['price'] != 'NA' else 'NA'
            self.location:str = initial_data['location']
            self.start_date:str = initial_data['started']
            self.finish_date:str = initial_data['finished']
            # replace these with functions to calculate them in python, rather than taking from excel
            # can only caluclate if dates aren't NA
            self.drink_velocity:int|str = self.calculate_drink_velocity()
            self.time_to_start:int|str = self.calculate_time_to_start()

    
        self.cans:list[Can] = []

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
        conn, curs = self._create_connection()
        associated_cans = [info[0] for info in curs.execute('SELECT can_id FROM can_data WHERE can_id LIKE ?', (f'%{self.box_id}%',))]
        self._close_commit(conn)
        return associated_cans
    

        

    def db_insert(self):
        package = [self.__dict__[parameter] for parameter in self.__dict__][self._base_class_parameter_amt+1:-2]
        self.add_box(tuple(package))

    def _retrieve_data(self):
        """Fills object with existing data from DB"""
        conn, curs = self._create_connection()
        box_data = [info for info in curs.execute(f'SELECT * FROM {self.box_table} WHERE box_id=?', (self.box_id,))][0]
        self._close_commit(conn)

        for data_parameter, value in zip(self.CLASS_PARAMS[:-1], box_data):
            self.__dict__[data_parameter] = value
        return


def upload_to_db():
    for data in ALL_BOX_DATA_DF:
        box_obj = Box(data['bid'], data)
        box_obj.db_insert()
    print('done uploading box data to DB')

    for data in ALL_CAN_DATA_DF:
        can_obj = Can(data['Can'], data)
        can_obj.db_insert()
    print('done uploading can data to DB')
    return


def get_table_property(table: Literal['box_data', 'can_data'], property:Literal['<database column>'], where_row:Literal['<database column>']=None, where_value:str|bool|int=None, count:bool=False):
    """Search for property from all specimens or specify a specific row"""
    conn, curs = database._create_connection()

    if where_row:
        data = [info[0] for info in curs.execute(f'SELECT {property} FROM {table} WHERE {where_row}=?', (where_value,))]
    else:
        data = [info[0] for info in curs.execute(f'SELECT {property} FROM {table}')]
    
    database._close_commit(conn)
    
    if count:
        return len(data)
    return data

def get_multiple_table_properties(table: Literal['box_data', 'can_data'], count:bool=False, *properties:list[str], **where_specifiers) -> list[tuple] | int:
    conn, curs = database._create_connection()

    data = [info for info in curs.execute(f'SELECT {','.join(properties)} FROM {table}')]
    if where_specifiers:
        where_data = database._generate_where_stmt(where_specifiers)

        data = [info for info in curs.execute(f'SELECT {','.join(properties)} FROM {table} WHERE {where_data['stmt']}', tuple(where_data['values']))]

    database._close_commit(conn)

    if count:
        return len(data)
    return data
