"""  

"""

from .config import logger, Literal
from .preprocessor import PreProcessCSV, PreProcessesMD


SUPPORTED_MODES = {

}

class PreProcessingPipelines:
    def __init__(self, mode=Literal[], *, box_source_path:str=None, can_source_path:str=None):
        
        
        self.preprocessor_parameters = {
            'box_source_path': box_source_path,
            'can_source_path': can_source_path
        }


    def _init_pre_processors(self) -> tuple[PreProcessCSV, PreProcessesMD]:
        """  """
        pp_csv = PreProcessCSV(box_source_path=self.preprocessor_parameters['box_source_path'], can_source_path=self.preprocessor_parameters['can_source_path'])
        pp_md = PreProcessesMD(box_source_path=self.preprocessor_parameters['box_source_path'], can_source_path=self.preprocessor_parameters['can_source_path'])

        logger.info(f'Initialized PreProcessors with parameters: {self.preprocessor_parameters}')

        return pp_csv, pp_md
    
    def execute_mode(self):
        """
        """
        processors = self._init_pre_processors()

        for processor in processors:
            processor.run_pre_processing()

    def _mode_






# RUN_PRE_PROCESSING (RPP) PIPELINES
def rpp_clean_complete_override():
    """ EXECUTION FUNCTION for entire pre processing pipeline
    
    Currently processes all data in the local csv_raw and md_raw directories. Processing
    entails data extraction from csv or md source data (obtained from Notion Database Table
    exports) and harmonization to a single csv file for further analysis.

    Exports to the local spreadsheets/processed directory. Each file is unique to the day only,
    meaning multiple runs on the same day will write to the same file, overriding previous runs 
    that day. 
    
    
    """
    pp_csv = PreProcessCSV()
    pp_md = PreProcessesMD()

    pp_csv.run_pre_processing()
    # pp_csv.display_run_stats()
    pp_md.run_pre_processing()
    # pp_md.display_run_stats()

    pp_csv.export_data(override_existing_content=['*']) # necessary for the first export to clear what came before
    pp_md.export_data()
    print(f'Finished processing and exporting CSV and MD data')
    return

