import DataAnalysis as da
import DataAnalysis.config as dac

import DataAnalysis.database as dadb
import DataAnalysis.basics as dab
import DataAnalysis.database.utils as dadu
import DataAnalysis.database.init_sequence as dais
import DataAnalysis.database.pp_recover as ddpp
# import DataAnalysis.database.pp as ddpp


import os
DB_DATA_DIR = r'C:\Users\tmalo\Desktop\GitHub\LCT\DataAnalysis\spreadsheets'

LCD = r'C:\Users\tmalo\Desktop\La Croix Data'
CDD = os.path.join(LCD, 'csv_raw')
MDD = os.path.join(LCD, 'md_raw')


if __name__ == '__main__':
    # ddpp.rpp_clean_complete_override()
    # pds = dadu.process_db_config()
    # dais.run(recreate_tables=True, fill_data=True)

    pp = ddpp.PreProcessor()
    
    # NOTE original implementation per last save
    # pp.run_pre_processing(
    #     type='csv',
    #     box_data_csv_path=os.path.join(DB_DATA_DIR, 'csv_raw', 'box_data.csv'),
    #     can_data_dir_path=os.path.join(DB_DATA_DIR, 'csv_raw', 'can_data_by_box'),
    #     md_data_dir=os.path.join(DB_DATA_DIR, 'md_raw', 'can_data_by_box')
    # )

    # pp.display_run_stats()
    # pp.export_to_file()


    # NOTE new implementation

    pp.run_pre_processing(csv_data_dir=CDD, md_data_dir=MDD)
    
    # print(pp.source_data_attributes['boxes_all']['data'], end='\n\n\n')
    # print(pp.source_data_attributes['boxes_flavor']['data'], end='\n\n\n')
    # print(pp.source_data_attributes['can_data']['data'], end='\n\n\n')

    pp.source_data_attributes['boxes_all']['data'].info()
    pp.source_data_attributes['boxes_flavor']['data'].info()
    pp.source_data_attributes['can_data']['data'].info()

    


    
        
    