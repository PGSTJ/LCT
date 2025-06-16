# import DataAnalysis as da
# import DataAnalysis.config as dac

from DataAnalysis.database.run import DAUtilPipelinePresets
from DataAnalysis.database.tools.static_analyses import update_static_analyses
# from DataAnalysis.database.utils.registry import DatabaseRegistry



if __name__ == '__main__':
        
    # pipelines = DAUtilPipelinePresets()
    # pipelines.run_preset(1) # NOTE recreate databases; fills reference.db and raw_data.db

    # TODO working on static_analysis.py
    
    update_static_analyses()

