
from ...utils import PickleHandler
from ...config import SAVED_TABLE_DATA_DIR, DB_DIR

from .base import Database, logging, os, Literal
from .custom_types import TableData


logger = logging.getLogger('standard')
pickler = PickleHandler()


def _check_for_saved_table_data(output_dir_path:str, decision_behavior:Literal['recent', 'choose']='recent') -> None|str:
    """ Checks for any pickled table data from previous extractions in the provided directory.
    
        Args:
            output_dir_path (str) : Path to the directory being search for saved table data
            decision_behavior (str) : Determines handling of multiple detected saves. Default is most recent.
                - *recent*: Choose the most recently created save
                - *choose*: Allows the user to manually choose the save to use

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
    
    if decision_behavior == 'recent':
        fps = [os.path.join(output_dir_path, fn) for fn in found_saves]
        mrf = max(fps, key=os.path.getctime)
        logger.info(f'Loading most recent save from {mrf}')
        return mrf 

    # assumes behavior to be manual user decision 
    print(f'Multiple saves detected. Choose from one below:')
    
    for idx, save in enumerate(found_saves, start=1):
        print(f'- ({idx}) {save}')
    
    option = input('Option: > ')
    assert option in found_saves, f'Invalid option: {option}. Must choose from below:'

    fp = os.path.join(output_dir_path, found_saves[found_saves.index(option)])
    return fp



class DatabaseRegistry:
    """ Global registry where database instances are stored 
    
        Args:
            base_database_dir (str) : Override the default base database directory 
            search_for_existing (bool) : If true, will search the base database directory 
                                         for existing databases to register
    
    
    """
    _instances:dict[str,Database] = {}
    _database_home_directory:str = str(DB_DIR)
    _saved_table_data_directory:str = str(SAVED_TABLE_DATA_DIR)


    def __init__(self, *, base_database_directory:str|None=None, saved_table_data_directory:str|None=None, search_for_existing:bool=False):
        if isinstance(base_database_directory, str):
            self._assign_database_home_directory(base_database_directory)

        if isinstance(saved_table_data_directory, str):
            self._assign_table_save_directory(saved_table_data_directory)

        if search_for_existing:
            self.search_and_register_dbs()



    @classmethod
    def _assign_database_home_directory(cls, path:str):
        assert os.path.isdir(path), f'Expected a path to a directory, not: {path}'
        cls._database_home_directory = path
        return
    
    @classmethod
    def _assign_table_save_directory(cls, path:str):
        assert os.path.isdir(path), f'Expected a path to a directory, not: {path}'
        cls._saved_table_data_directory = path
        return


    @classmethod
    def search_and_register_dbs(cls):
        """ Searches directory for .db files and registers them if not already. 
            
            Of note, databases registered in this way will not have any table_data 
            registered unless it is adding manually later.

            Args:
                db_dir (str) : Path to a directory potentially containing databases
                saved_table_data_dir (str) : Path to a directory potentially containing 
                                             previously saved table data
        
        """
        db_dir = cls._database_home_directory
        
        valid_files = [i for i in os.listdir(db_dir) if i.endswith('.db')]
        
        if len(valid_files) == 0:
            logger.warning(f'No databases found in this directory: {db_dir}')
            return
        
        logger.info(f'Identified {len(valid_files)} databases to register in {db_dir}')

        # check for previous saves to apply semi automatically
        save_path = _check_for_saved_table_data(cls._saved_table_data_directory)

        # iterate through valid database files and create a new object
        # automatically unpacks and assigns table data from a previous save if available
        for db in valid_files:
            dbn = db[:-3]
            new_db = Database(database_name=dbn)
            
            if save_path:
                td:dict[str, TableData] = pickler.load_pickle(save_path)

                if dbn in td:                
                    new_db.add_table_data(td[dbn], if_exist='ignore')
                else:
                    logger.warning(f'This database ({dbn}) does not have previously saved table data in the current save: {save_path} ')

        
            cls.add_instance(new_db)

        return



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
    def view_instances(cls, *, view_header:str|None=None):
        """ Display registed databases 
        
            Args:
                view_header (str) : Optionally add a header for this viewing. Prints above the view_instance() report.
        
        """
        if view_header:
            print(view_header)
        
        total_reg = len(cls._instances)
        print(f'Total Databases Registed: {total_reg}')
        
        if total_reg == 0:
            cls.verify_true_db_existence(cls._database_home_directory)
            return
        
        for db_name in cls._instances:
            print(f'\t- {db_name}')

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
    def validate_registration(cls, db_name:str) -> bool:
        """ Checks the global registry for a database registered with the provided name 
        
            Args:
                db_name (str) : The name of the database in the registry

            Returns:
                True if the name exists in the registry
        
        """

        if db_name in cls._instances.keys():
            return True
        return False

    @classmethod
    def reset_databases(cls, database_names:tuple[str]|None=None):
        """ Deletes tables and removes database(s) from registry

            Args: 
                database_names (tuple[str]) : Sequence of string names corresponding to database names
                                              in the global registry
        
        
        """
        queue = cls.drop_db_tables(database_names=database_names)
        for dbn, _ in queue:
            cls._instances.pop(dbn)
            logger.info(f'Removed database ({dbn}) from the registry')

        return

        

    @classmethod
    def drop_db_tables(cls, database_names:tuple[str]|None=None) -> list[tuple[str, Database]]:
        """ Drops all tables of all databases and returns a collection for optional use
        
            Args:
                database_names (tuple[str]) : Sequence of string names corresponding to database names
                                              in the global registry

            Returns:
                The queue for any future actions (i.e. removing from registry during reset)
        
        """
        assert len(cls._instances) > 0, f'No registered databases to drop'

        # selects any provided databases, assuming all are valid
        if database_names:
            assert all(name in cls._instances for name in database_names), f'Invalid database name(s) provided: {[nm for nm in database_names if nm not in cls._instances]}'
            queue = [(dbn, dbi) for dbn,dbi in cls._instances.items() if dbn in database_names]
        else:
            queue:list[tuple[str, Database]] = [(dbn, dbi) for dbn,dbi in cls._instances.items()]


        for _, dbi in queue:
            dbi.drop_tables()
        return queue
    
    @staticmethod
    def verify_true_db_existence(db_home_dir:str|None):
        """ Checks for existence in provided home database dir for potentially unregistered databases 
        
            Should only be called if the zero registered databases were detected.
        
        """
        if not db_home_dir:
            return
        
        assert os.path.exists(db_home_dir), f'DB home directory path doesnt exist: {db_home_dir}'
        assert os.path.isdir(db_home_dir), f'Must supply a path to a directory, not: {db_home_dir}'

        files = [file for file in os.listdir(db_home_dir) if file.endswith('.db')]
        detected_files = len(files)

        if detected_files > 0:
            print(f'{detected_files} database files were detected in the DB home directory: {db_home_dir}')
            for fl in files:
                print(f'\t- {fl[:-3]}')
        
        return
        
        
        