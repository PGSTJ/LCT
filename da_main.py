import DataAnalysis as da
import DataAnalysis.config as dac

import DataAnalysis.database as dadb
import DataAnalysis.basics as dab
import DataAnalysis.database.utils as dadu
import DataAnalysis.database.init_sequence as dais
import DataAnalysis.database.pre_processing as ddpp
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



DB_DATA_DIR = r'C:\Users\tmalo\Desktop\GitHub\LCT\DataAnalysis\spreadsheets\processed'


if __name__ == '__main__':
    ddpp.rpp_clean_complete_override()
    pds = dadu.process_db_config()
    dais.run(recreate_tables=True, fill_data=True)

    
    # import os

    # d = [os.path.join(DB_DATA_DIR,file) for file in os.listdir(DB_DATA_DIR) if file.endswith('2025.csv')]

    # import pandas as pd
    # dfs = [pd.read_csv(fp) for fp in d]
    # print(dfs)
        
    