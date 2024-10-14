import DataAnalysis
import DataAnalysis.basics
import DataAnalysis.config
import DataAnalysis.format_upload_data

import DataAnalysis.database as dadb
import DataAnalysis.utils as dau




if __name__ == '__main__':
    dadb.Database()._recreate_tables()
    dau.upload_to_db()
    
      
    
    
