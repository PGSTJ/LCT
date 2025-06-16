# import DataAnalysis as da
# import DataAnalysis.config as dac

from DataAnalysis.database.run import DAUtilPipelinePresets
# from DataAnalysis.database.tools.dynamic_analysis import update_flavor_analysis
# from DataAnalysis.database.utils.registry import DatabaseRegistry



if __name__ == '__main__':
        
    pipelines = DAUtilPipelinePresets()
    pipelines.run_preset(1) # NOTE recreate databases; fills all databases in order: reference -> raw_data -> static_analyses -> dynamic_analyses

    # TODO working on static_analysis.py
    
