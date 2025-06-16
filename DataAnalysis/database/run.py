
from ..config import EXTERNAL_DATA_DIR, SAVED_TABLE_DATA_DIR


from .utils.general import create_reset_databases, DatabaseRegistry
from .tools.reference import upload_reference_data
from .tools.raw_data import process_and_export_lc_data
from .tools.dynamic_analysis import default_fill_general_table, update_table_general, update_flavor_analysis
from .tools.static_analyses import update_static_analyses



 





class DAUtilPipelinePresets:
    """ Various preserved states of common run sequences """
    _total_presets:int = 0

    @classmethod
    def run_preset(cls, preset:int):
        if preset == 1:
            cls.complete_database_reset()
            cls.extract_and_update_databases()
            return
        elif preset == 2:
            print(f'\t\tPreset 2 - Extracting raw data and updating static/dynamic analyses')
            cls.extract_and_update_databases()
        
        else:
            raise ValueError(f'Invalid preset {preset}. Choose between 1 and {cls._total_presets}')
        
        return
                

    @staticmethod
    def complete_database_reset():
        """ Resets databases, fills default data and extracts csv/md data currently in directories

            Order of operations:
                1. Reset databases
                2. Upload reference data
                3. Fill general table of dynamic_analysis.db with default data
                4. Extract box/can data from CSV/MD directories
                5. Update general table with extracted values from Processor returned in operation 4
        
        
        
        """
        print(f'\t\tPreset 1 - Resets Databases, processes raw data, and fills all databases')

        create_reset_databases(reset=True, save_table_data=True, output_dir_path=SAVED_TABLE_DATA_DIR)
        upload_reference_data()
        default_fill_general_table()

        return
    
    @staticmethod
    def something():
        """ Completely resets databases; deleting then re-filling everything
        
        
        """
        db_reg = DatabaseRegistry()
        rawdata = db_reg.get_instance('raw_data')

        rawdata.drop_tables()
        rawdata.create_tables()

        process_and_export_lc_data(
            base_data_dir=str(EXTERNAL_DATA_DIR),
            display_processing_stats=True,
            db_export=True
        )

    @staticmethod
    def extract_and_update_databases():
        """ Process Exports and update analyses databases

            Must be executed in this order. Specifically,
            static_analyses must always come before table_general
        
        
        """
        process_and_export_lc_data(
            base_data_dir=str(EXTERNAL_DATA_DIR),
            display_processing_stats=True,
            db_export=True
        )

        update_static_analyses()
        update_table_general()
        update_flavor_analysis()

        return
        