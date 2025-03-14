import DataAnalysis as da
import DataAnalysis.config as dac

import DataAnalysis.database as dadb
import DataAnalysis.basics as dab
import DataAnalysis.database.utils as dadu
import DataAnalysis.database.init_sequence as dais
import DataAnalysis.database.pre_processing as ddpp

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


if __name__ == '__main__':
    # dadb.Database()._recreate_tables()
    # dais.run(fill_data=False)

    
   ddpp.rpp_clean_complete_override()

    
