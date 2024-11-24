import DataAnalysis
import DataAnalysis.basics as dab
import DataAnalysis.config as dac
import DataAnalysis.format_upload_data as dafud

import DataAnalysis.database as dadb
import DataAnalysis.utils as dau

db = dadb.Database()

def recreate_db_tables_and_data():
    dadb.Database()._recreate_tables()
    dau.upload_to_db()
    return


if __name__ == '__main__':    
    d = dau.Box('39GSP')
    print(d.fill_can_data())

    
    #TODO update canid/box id mapping for 2023 data to 2024 convention
    
    
