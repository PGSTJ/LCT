# import DataAnalysis as da
# import DataAnalysis.utils as dau
import DataAnalysis.config as dac

import DataAnalysis.database.run as dadr

# import DataAnalysis.database.pp as ddpp


import os



if __name__ == '__main__':
    # d = dadu.update_static_analyses()    
        
    # TODO in progresss of filling static analysis DB
    # rn need to manually add table date with format -> process from utils
    # then DatabaseRegistry.get_instance() will workp
    
    # dadr.create_reset_databases(save_table_data=True, output_dir_path=dac.SAVED_TABLE_DATA_DIR)
    dadr.process_and_export_lc_data(
        base_data_dir=dac.EXTERNAL_DATA_DIR,
        db_export=True,
        db_override=True
    )    