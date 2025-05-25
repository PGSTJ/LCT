import DataAnalysis as da
import DataAnalysis.config as dac

import DataAnalysis.database as dadb
import DataAnalysis.basics as dab
import DataAnalysis.database.utils as dadu
import DataAnalysis.database.init_sequence as dais
import DataAnalysis.database.pp_recover as ddpp
# import DataAnalysis.database.pp as ddpp

CAN_DATA_HEADER = [
    'Can',
    'Initial Mass',
    'Initial Volume',
    'Final Mass',
    'Final Volume',
    'Finished',
    'Box'
]

BOX_DATA_HEADER = [
        'bid',
        'flavor',
        'purchase_date',
        'price',
        'location',
        'started',
        'finished',
        'DV',
        'TTS'
    ]


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


    # NOTE testing recovered implementation

    bd = pp.run_pre_processing(csv_data_dir=CDD, md_data_dir=MDD)
    

    cdbb = os.path.join(CDD, 'can_data_by_box')
    # print(f'original len: {len(bd)}')        
    # print(f'bfdf len: {len(bfdf)}')
    # print(bfdf)
    # files = [f[:-4] for f in os.listdir(cdbb)]
    # missing = [n for n in bd['og_id'] if n not in files]
    # print(len([k for k,_ in cd.items()]))
    # TODO need to track boxes without can data to avoid validation errors
    
    # NOTE testing can data box extraction
    
    
    bad = pp.source_data_attributes['boxes_all']['data']
    bfd = pp.source_data_attributes['boxes_flavor']['data']

    
    


    
        
    