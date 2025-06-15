
from ..config import EXTERNAL_DATA_DIR, SAVED_TABLE_DATA_DIR


from .utils.general import create_reset_databases
from .tools.reference import upload_reference_data
from .tools.raw_data import process_and_export_lc_data



 





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
        """ Resets Databases, processes raw data and uploads to database 
        
        
        
        """
        print(f'Preset 1 - Resets Databases, processes raw data and uploads to database')

        create_reset_databases(reset=True, save_table_data=True, output_dir_path=SAVED_TABLE_DATA_DIR)
        upload_reference_data()
        process_and_export_lc_data(
            base_data_dir=str(EXTERNAL_DATA_DIR),
            display_processing_stats=True,
            db_export=True
        )
        return
   