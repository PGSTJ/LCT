from ... import Literal, logging, os
from utils.registry import DatabaseRegistry

from utils.processor import DataProcessor



logger = logging.getLogger('standard')
db_reg = DatabaseRegistry()


def process_and_export_lc_data(
        base_data_dir:str, 
        filter_export_collections:tuple[str]=('*',),
        *, 
        display_processing_stats:bool=False,
        db_export:bool=False, 
        db_override:bool = False,
        file_export:Literal['csv', 'pickle']|None=None,
        output_dir_map:dict[str, str]|None=None,
        output_file_map:dict[str, str]|None=None,
    ):
    """ Processes all raw data in La Croix Data directory and exports to a database or specified file

        Args:
            base_data_dir (str) : Path to the directory containing raw CSV and MD data
            filter_export_collections (tuple[str]) : Optionally filter for specific collections to export.
                                                     Default is "*", which exports all collections. 
                                                                                            
            display_processing_stats (bool) : If true, prints various stats about the processing run. Default 
                                              is False.
            db_export (bool) : If true, exports newly processed data to the database (currently 'raw_data.db'). 
                               Default is False. Must supply a DatabaseRegistry if True.
            database_registry (DatabaseRegistry) : Master collection of created and registered databases. Only
                                                   required if db_export=True
            db_override (bool) : If true, will override any existing contents in the tables. Default is False.
            file_export (Literal['csv', 'pickle']) : Specifies file type to export newly processed data to.
            output_dir_map (dict[str, str]) : Override default output directory locations. Only required if
                                              all=False AND output_dir_map is None.                            
            output_file_map (dict[str, str]) : Override default output file names.
    
    """
    assert os.path.exists(base_data_dir), f'Base data dir path does not exist: {base_data_dir}'
    assert os.path.isdir(base_data_dir), f'Must supply a path to a directory, not: {base_data_dir}'

    # NOTE cannnn generalize to '_raw' presence, but being explicit is probably better
    req_subdirs = ['csv_raw', 'md_raw']
    valid_subdirs = [i for i in os.listdir(base_data_dir) if i in req_subdirs]
    assert len(valid_subdirs) == 2, f'Base data dir does not contain the required sub-directories: csv_raw or md_raw'

    csv_data, md_data = tuple([os.path.join(base_data_dir, rsd) for rsd in req_subdirs])

    procesor = DataProcessor()
    procesor.run_pre_processing(csv_data_dir=csv_data, md_data_dir=md_data)

    if display_processing_stats:
        procesor.display_run_stats()

    if db_export:
        assert db_reg, f'Must supply a DatabaseRegistry to export to database'
        raw_data_db = db_reg.get_instance('raw_data')
        
        procesor.db_export(raw_data_db, filter_export_collections, override=db_override)

        logger.info('Finished exporting to database')
    
    if file_export:
        all_data = True if filter_export_collections == '*' else False
        procesor.file_export(
            type=file_export,
            all_data=all_data,
            collections=filter_export_collections,
            output_dir_map=output_dir_map,
            output_file_map=output_file_map
        )

        logger.info(f'Finished exporting data to {file_export}')

    return
   