
from .base import Database, logging, os


logger = logging.getLogger('standard')


class DatabaseRegistry:
    """ Global registry where database instances are stored """
    _instances:dict[str,Database] = {}
    database_home_directory:str|None = None

    def __init__(self, *, base_database_dir:str|None=None):
        if base_database_dir:
            self._assign_database_home_directory(base_database_dir)



    @classmethod
    def _assign_database_home_directory(cls, path:str):
        cls.database_home_directory = path
        cls.search_and_register_dbs(cls, path)
        return


            # TODO CONSIDER ADDING FUNCTION TO AUTO REGISTER ANY EXISTING DATABASES IN THE HOME DIR
    def search_and_register_dbs(self, path:str):
        """ Searches directory for .db files and registers them if not already. 
            
            Of note, databases registered in this way will not have any table_data 
            registered unless it is adding manually later.

            Args:
                path (str) : Path to a directory potentially containing databases
        
        """
        assert os.path.isdir(path), f'Expected a path to a directory, not: {path}'
        
        valid_files = [i for i in os.listdir(path) if i.endswith('.db')]
        
        if len(valid_files) == 0:
            logger.warning(f'No databases found in this directory: {path}')
            return
        
        logger.info(f'Identified {len(valid_files)} databases to register in {path}')

        for db in valid_files:
            new_db = Database(database_name=db[:-3])
            self.add_instance(new_db)
        
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
            cls.verify_true_db_existence(cls.database_home_directory)
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

        # queues either all or only specified  databases from the registry
        queue:list[tuple[str, Database]] = [(dbn, dbi) for dbn,dbi in cls._instances.items()] if not database_names else [(dbn, dbi) for dbn,dbi in cls._instances.items() for dbn in database_names]

        for dbn, dbi in queue:
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
        
        
        