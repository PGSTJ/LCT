import DataAnalysis as da
import DataAnalysis.utils as dau
import DataAnalysis.config as dac

import DataAnalysis.database as dadb
import DataAnalysis.basics as dab
import DataAnalysis.database.utils as dadu
import DataAnalysis.database.processor as ddpp
# import DataAnalysis.database.pp as ddpp


import os



LCD = r'C:\Users\tmalo\Desktop\La Croix Data'
CDD = os.path.join(LCD, 'csv_raw')
MDD = os.path.join(LCD, 'md_raw')


config_yaml = r'C:\Users\tmalo\Desktop\GitHub\LCT\DataAnalysis\database\config_sheets\processing_config.yaml'


if __name__ == '__main__':
    # ddpp.rpp_clean_complete_override()
    # pds = dadu.process_db_config()
    # dais.run(recreate_tables=True, fill_data=True)


    pp = ddpp.DataProcessor()
    
    pp.run_pre_processing(csv_data_dir=CDD, md_data_dir=MDD)
    
    pp.display_run_stats()
    pp.export_to(type='csv')
    
        
    