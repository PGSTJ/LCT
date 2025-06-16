
from ..config import EXTERNAL_DATA_DIR, SAVED_TABLE_DATA_DIR


from .utils.general import create_reset_databases
from .utils.registry import DatabaseRegistry
from .tools.reference import upload_reference_data
from .tools.raw_data import process_and_export_lc_data
from .tools.dynamic_analysis import default_fill_general_table, update_table_general



 





class DAUtilPipelinePresets:
    """ Various preserved states of common run sequences """
    _total_presets:int = 0

    @classmethod
    def run_preset(cls, preset:int):
        if preset == 1:
            cls.preset_one()
        
        else:
            raise ValueError(f'Invalid preset {preset}. Choose between 1 and {cls._total_presets}')
        
        return
                

    @staticmethod
    def preset_one():
        """ Resets databases, fills default data and extracts csv/md data currently in directories

            Order of operations:
                1. Reset databases
                2. Upload reference data
                3. Fill general table of dynamic_analysis.db with default data
                4. Extract box/can data from CSV/MD directories
                5. Update general table with extracted values from Processor returned in operation 4
        
        
        
        """
        print(f'Preset 1 - Resets Databases, processes raw data and uploads to database')

        create_reset_databases(reset=True, save_table_data=True, output_dir_path=SAVED_TABLE_DATA_DIR)
        upload_reference_data()
        default_fill_general_table()
        process_and_export_lc_data(
            base_data_dir=str(EXTERNAL_DATA_DIR),
            display_processing_stats=True,
            db_export=True
        )
        update_table_general()

        return
    

    def preset_two():
        """
        
        
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

        