import DataAnalysis as da
import DataAnalysis.basics
import DataAnalysis.config as dac

import DataAnalysis.database as dadb
import DataAnalysis.database
import DataAnalysis.database.init_sequence as dais
import DataAnalysis.basics as dab
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


if __name__ == '__main__':
    dadb.Database()._recreate_tables()
    dais.run()
