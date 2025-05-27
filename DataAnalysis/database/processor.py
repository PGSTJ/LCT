
from typing import Literal, TypeAlias

from .. import (logging, os, pd, re, io, datetime, np, generate)
from ..utils import read_yaml_data, get_current_time
from ..config import (
    PRIM_DATETIME_FORMAT, DB_CONFIG_DIR, DEFAULT_PROCESSING_OUTPUT_DIR
)

logger = logging.getLogger('standard')


CONFIG_DATA = read_yaml_data(DB_CONFIG_DIR / 'processing_config.yaml')


class DataProcessor:
    """ Utility class for extracting and processing la croix data into databases 
    
    Attributes
    ----------

        Helper Attrs:
            id_map (dict[str, dict[str,str]]) : Maps the original box id to the generated box ids (for purchase and flavors).
                                                            Used for post-processing relation linking, validation, and 
                                                            verification purposes.

        Output Attrs:
            box_purchases (pd.Dataframe) : Dataframe containing extracted and formatted box purchase data
            box_flavors (pd.Dataframe) : Dataframe containing extracted and formatted box flavor data
            can_data (pd.Dataframe) : Dataframe containing extracted and formatted can data


    Methods
    -------
        run_pre_processing(csv_data_dir:str, md_data_dir:str)
    
    """
    def __init__(self):
        self.id_map:dict[str, dict[Literal['purchase id', 'flavor id'], str]] = {} # maps og_id to purchase and flavor ids

        # transient tracking attributes
        self.extracted_box_data:list[pd.DataFrame] = []
        self.extracted_can_data:dict[str, pd.DataFrame] = {}

        # final dataframes
        self.box_purchases_df:pd.DataFrame|None = None
        self.box_flavors_df:pd.DataFrame|None = None
        self.can_data_df:pd.DataFrame|None = None

        self.alias_map:dict[str,str] = self._create_data_alias_map() # keys are collection aliases specified in processing_config.yaml

        self.metadata = {
            'Process Execution Time': datetime.datetime.now().strftime(PRIM_DATETIME_FORMAT),
            'Export': {}
        }
        
        
    # @@@@@@@@@@@@@@@@@@@ METHODS @@@@@@@@@@@@@@@@@@@

    # NOTE probably more gen background util than true method purposes
    def get_data_collections(self, all_data:bool=True, data_collection:tuple[str]=None) -> dict[str, pd.DataFrame]:
        """ Get dataframe of processed data

        Args
        ----
            all_data (bool) : If True, will return all data collections, mapped from their database table name. Default
                              is True. Must specify at least one collection if False.
            data_collection (tuple[str]) : Sequence containing valid aliases of specific dataframes to pull. Default 
                                           is None. Valid aliases are specified in processing_config.yaml under 'data_aliases'.

        Returns
        -------
            A dictionary mapping the collection alias to the dataframe
        
        """
        

        if all_data:
            return {collection_alias: self.__dict__[df_key] for collection_alias, df_key in self.alias_map.items()}

        assert data_collection, f'Must specify at least one collection to return if all_data=False'
        assert all(collection_alias in self.alias_map for collection_alias in data_collection), f'Invalid collection alias specified. Must be one of {list[self.alias_map.keys()]}'

        return {collection_alias:self.__dict__[self.alias_map[collection_alias]] for collection_alias in data_collection}
        
        

    

    def _create_data_alias_map(self) -> dict[str,str]:
        """ Maps the collection aliases specified in the config YAML to the corresponding dataframe data collection """
        
        aliases:list[str] = [aliases for _, aliases in CONFIG_DATA['data_aliases'].items()]        
        all_collections:list[pd.DataFrame] = [k for k in self.__dict__ if k.endswith('_df')]
        alias_map = dict(zip(aliases, all_collections))

        logger.info(f'Created alias map for data collections from aliases in config file') # TODO add some kind of sanity check to ensure aliases map to the correct dataframe

        return alias_map

    def run_pre_processing(
            self,
            csv_data_dir:str=None,
            md_data_dir:str=None
        ):
        """ Extracts Box and Can data from CSV or (notion export) MD files and formats for DB insertion
        
        """
        assert csv_data_dir or md_data_dir, f'Must supply at least one directory to process'
        
        if csv_data_dir:
            box_data_df, can_data_dfs = self._extract_csv_data(csv_data_dir)
            self.extracted_box_data.append(box_data_df)
            self.extracted_can_data.update(can_data_dfs)

        if md_data_dir:
            box_data_df, can_data_dfs = self._extract_md_data(md_data_dir)
            self.extracted_box_data.append(box_data_df)
            self.extracted_can_data.update(can_data_dfs)
        
        all_box_data = pd.concat(self.extracted_box_data, ignore_index=True)

        self._process_box_data(all_box_data)
        self._process_can_data()

        return

    # @@@@@@@@@@@@@@@@@@@ PROCESSING BACKGROUND UTILS @@@@@@@@@@@@@@@@@@@

    # NOTE headers used in extraction will change once new table format is fully implemented TODO 
    def _extract_csv_data(self, data_dir:str) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """ Extract box/can data from CSV files and normalizes format
        
        Returns
        -------
            Tuple of box and can data, where box data is a Dataframe and can data is a DataframeGroupBy collection
        
        """
        box_data_path = os.path.join(data_dir, 'box_data.csv')
        can_data_path = os.path.join(data_dir, 'can_data_by_box')

        box_data_df = pd.read_csv(box_data_path)
        norm_box_data_df = self._normalize_box_df(box_data_df, type='csv')


        can_data_dfs = {file[:-4]:self._normalize_can_df(pd.read_csv(os.path.join(can_data_path, file)), 'csv') for file in os.listdir(can_data_path)}

        return norm_box_data_df, can_data_dfs

    def _extract_md_data(self, md_data_dir:str) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """ Extract box/can data from MD files and normalizes format
        
        Returns
        -------
            Tuple of box and can data, where box data is a Dataframe and can data is a DataframeGroupBy collection
        
        """
        all_data = os.path.join(md_data_dir, 'can_data_by_box')

        box_data_dicts = []
        can_data_dict = {}

        for md_file in os.listdir(all_data):
            og_id = md_file.split(' ')[0] 
            props, table_data = self._read_markdown_data(os.path.join(all_data, md_file))
            logger.debug(f'\nExtracted property data: {props}')

            # extract/collect box data
            formatted_box_properties = self._box_header_format_converter(props)
            formatted_box_properties['og_id'] = og_id
            box_data_dicts.append(formatted_box_properties)

            if table_data is not None:
                norm_can_df = self._normalize_can_df(table_data, 'md')
            else:
                norm_can_df = table_data
                logger.warning(f'No can data for {og_id}')

            can_data_dict[og_id] = norm_can_df

        box_df = pd.DataFrame(box_data_dicts)
        norm_box_df = self._normalize_box_df(box_df, 'md')

        return norm_box_df, can_data_dict

    def _process_box_data(self, box_data_df:pd.DataFrame):        
        # extract/format box_all data
        all_badf = []
        
        for base_og_id, df in box_data_df.groupby('base_og_id'):   
            box_id = generate(size=7)     
            intermediate_ba_df = df[['purchase_date', 'price', 'location']].drop_duplicates() # TODO add column extraction to dedicated CONSTANT file
            intermediate_ba_df.insert(0, 'base_og_id', base_og_id)
            intermediate_ba_df.insert(0, 'id', box_id)
            all_badf.append(intermediate_ba_df)

            
        box_all_df = pd.concat(all_badf, ignore_index=True)
        self.resolve_dtypes(box_all_df)
        self.box_purchases_df = box_all_df


        # extract/format box_flavor data
        bf_df = box_data_df[CONFIG_DATA['headers_to_extract']['flavor']] 

        # get box id from ba df based on poi, inserted at beginning
        bf_df.insert(0, 'box_id', bf_df['base_og_id'].apply(lambda x:self._get_box_id(df= box_all_df, base_original_box_id=x)))
        bf_df.insert(0, 'id', [generate(size=7) for _ in range(len(bf_df))])
        # bf_df['has_cans'] = False # TODO add column to headers for DB insertion
        bf_df.insert(len(bf_df.columns), 'has_cans', False) # TODO add column to headers for DB insertion

        # create id map before dropping unnecessary cols for box flavor DF
        ids_to_delete = ['og_id', 'base_og_id']
        self._create_id_map(bf_df[ids_to_delete+['id']], box_all_df[['id', 'base_og_id']])

        box_flavor_df = bf_df.drop(columns=ids_to_delete)

        self.box_flavors_df = self.resolve_dtypes(box_flavor_df)

        logger.info(f'Successfully created and saved box dataframes')
        self.validate_ba_to_bf_difference() # NOTE still deciding if I want to call this here

        return

    def _process_can_data(self):
        """
        
        """
        all_flavors:pd.DataFrame = self.box_flavors_df

        processed_data = []
        for og_id, df in self.extracted_can_data.items():
            if df is None:
                continue

            # mark box as having cans
            flavor_id = self.id_map[og_id]['flavor id']
            row = all_flavors[all_flavors['id']==flavor_id].index[0]
            all_flavors.loc[row, 'has_cans'] = True

            # values for two missing can data df columns
            all_ids = self.id_map[og_id] # TODO REALLY IMPORTANT !!!! CHANGE "ALL" NOMENCLATURE TO "PURCHASE" FOR CLARITY
            id_col = df['Can'].apply(lambda x:f'{all_ids["purchase id"]}.{x}')
            
            df.insert(0, 'box_id', flavor_id)
            df.insert(0, 'id', id_col)
            df = df.drop(columns=['Can'])
            final_df = self.resolve_dtypes(df)
            processed_data.append(final_df)

        
        self.can_data_df = pd.concat(processed_data, ignore_index=True)
        return
            
    def _normalize_box_df(self, box_df:pd.DataFrame, type:Literal['md', 'csv']):
        """ Ensures extracted box df headers and format from each source are the same """
        if type == 'csv':
            box_df = box_df.rename(columns={'started':'start_date', 'finished':'finish_date'}) # honestly might be better to just rename the CSV itself since csv processing should be obsolete moving forward
            box_df['exceptions'] = np.nan # csv box data is old format -> "exceptions" wasn't implemented yet
        elif type == 'md':
            # change date format 
            date_cols = [col for col in box_df.columns if 'date' in col]
            for col in date_cols:
                box_df[col] = pd.to_datetime(box_df[col], errors='coerce', format='%B %d, %Y')
                box_df[col] = box_df[col].dt.strftime('%m/%d/%Y')

            # remove unnecessary columns
            box_df = box_df.drop(columns=['tracking'])

        box_df['base_og_id'] = box_df['og_id'].apply(self._get_base_ogid)
        

        return box_df
    
    @staticmethod # NOTE for now
    def _normalize_can_df(can_df:pd.DataFrame, type:Literal['md', 'csv']):
        """ Ensures extracted can df headers and format from each source are the same """
        hdrs_to_rename = can_df.columns.to_list()
        hdr_map = dict(zip(hdrs_to_rename[1:], CONFIG_DATA['headers_to_extract']['can']))
        can_df = can_df.rename(columns=hdr_map)

        if type == 'md':
            can_df = can_df.drop(index=0) # artifact from MD table notation

        return can_df

    @staticmethod
    def resolve_dtypes(df:pd.DataFrame):

        for col in df.columns:
            if col == 'price':
                df['price'] = df['price'].astype('float16')
            elif 'date' in col:    
                df[col] = pd.to_datetime(df[col])
            elif df[col].dtype == 'bool':
                continue
            elif 'mass' in col or 'volume' in col:
                df[col] = df[col].astype('float16')
            else:
                df[col] = df[col].astype('string')
        return df

    def _create_id_map(self, ids_from_bf:pd.DataFrame, ids_from_ba:pd.DataFrame):
        """ Maps the og id to generated box all (purchase) and flavor ids 
        
        Nested dictionary keys are "<flavor/purchase> id"
        
        
        """
        
        for id in ids_from_bf['og_id'].to_list():
            row = ids_from_bf[ids_from_bf['og_id']==id].to_dict(orient='records')[0]
            self.id_map[row['og_id']] = {'flavor id':row['id']}

        for ogid in self.id_map:
            base = self._get_base_ogid(ogid)
            ba_id = ids_from_ba[ids_from_ba['base_og_id']==base]['id'].to_list()[0]
            self.id_map[ogid]['purchase id'] = ba_id

        logger.info('Created ID map ')

        return
            
    @staticmethod
    def _get_base_ogid(full_og_id: str) -> str:
        """Extracts the pure original ID from the full ID."""
        match_ogid = re.match(r"(\d+)([A-Za-z]+)", full_og_id)
        number, flavor = match_ogid.groups()
        return number + flavor

    def _get_box_id(self, df:pd.DataFrame, base_original_box_id: str) -> str | None:
        """Gets the generated box ID from the collection."""
        try:
            return df.loc[df['base_og_id'] == base_original_box_id, 'id'].values[0]
        except IndexError:
            logger.error(f"{base_original_box_id} does not exist in the box_all collection")
            return None


    #                                   @@@@@@@@@@@@@@@@@@@ MD SPECIFIC PROCESSING BACKGROUND UTILS @@@@@@@@@@@@@@@@@@@

    @staticmethod
    def _read_markdown_data(file_path:str) -> tuple[dict[str,str], pd.DataFrame|None]:
        """Parses exported Notion Page markdown files into properties and description data; the latter of
        which is essentially table data given how LCT Notion Pages are designed.
        
        
        
        Returns
        -------
            A tuple containing box data as a dict and can data as a Dataframe, in that order
        
        """
        with open(file_path, 'r') as file:
            markdown_content = file.read()

        # Split content based on the first occurrence of a table (which starts with a pipe '|')
        parts = re.split(r'(\n\|.*?\n)', markdown_content, maxsplit=1)
        # return parts
        properties_section = parts[0]
        table_section = "".join(parts[1:]) if len(parts) > 1 else None

        # Extract properties into a dictionary (key-value pairs)
        properties = {}
        for line in properties_section.splitlines():
            if ": " in line:
                key, value = line.split(": ", 1)
                properties[key.strip()] = value.strip()
        

        # Extract table using pandas
        if isinstance(table_section, str):
            # Remove leading and trailing pipes and spaces from the table section
            cleaned_table_section = re.sub(r"^\s*\|\s*|\s*\|\s*$", "", table_section, flags=re.MULTILINE)
            table = pd.read_csv(io.StringIO(cleaned_table_section), sep=r"\s*\|\s*", engine='python')
        else:
            # raise Exception(f'Extracted Table section from MD was empty.\n{table_section = }\n{parts = }')
            table = None

        return properties, table

    def _box_header_format_converter(self, extracted_properties:dict[str,str]) -> dict[str,str]:
        """ Converts from MD header format to DB header format

        Only for extracting box data (properties) from Notion export 
        MD files. Property keys need to match current headers for 
        extracting data from CSVs.
        
        """
        manual:dict[str,str] = {  # keys should match key in extracted_properties
            'Purchased':'purchase_date',
            'Started':'start_date',
            'Finished':'finish_date'
            } 

        ep_formatted = {}

        
        for params in extracted_properties:
            ep_formatted[params.lower()] = extracted_properties[params]
        try:
            for extras in manual:
                ep_formatted[manual[extras]] = extracted_properties[extras]
                ep_formatted.pop(extras.lower())
        except KeyError as e:
            ep_formatted[manual[e.args[0]]] = 'NA'
        
        return ep_formatted



    # @@@@@@@@@@@@@@@@@@@ FILE EXPORTING @@@@@@@@@@@@@@@@@@@

    def export_to(
            self,
            type:Literal['csv', 'pickle', 'db'],
            all_data:bool=True,
            collections:tuple[Literal['purchases', 'flavors', 'cans']]=None,
            output_dir_map:dict[Literal['purchases', 'flavors', 'cans'], str]=None,
            output_file_map:dict[Literal['purchases', 'flavors', 'cans'], str]=None

    ) -> None:
        """ Exports the DataFrames to CSV files.
        
        Args
        ----
            type (Literal['csv', 'pickle', 'db']) : Type of file to export to.
            all (bool) : If True, exports all collections. Must specify at least one 
                         collection OR output_dir_map if False. Default is True.
            collections (tuple[Literal['purchases', 'flavors', 'cans']]) : Sequence of specific collections
                                                                           to export. Only required if 
                                                                           all=False AND output_dir_map 
                                                                           is None.
            output_dir_map (dict[Literal['purchases', 'flavors', 'cans'], str]) : Override default output 
                                                                                  directory locations. Only 
                                                                                  required if all=False AND 
                                                                                  output_dir_map is None.
            output_file_map (dict[Literal['purchases', 'flavors', 'cans'], str]) : Override default output 
                                                                                   file names. 

        Returns
        -------
            Nothing.
        
        """
        
        # start building export package with final dataframes
        generic_export_package = self._get_generic_export_queue()

        queue, metadata = self._build_export_queue(
            package=generic_export_package, 
            all_data=all_data, 
            collections=collections, 
            output_dir_map=output_dir_map, 
            output_file_map=output_file_map
        )

        self.metadata['Export'] = metadata # NOTE the first time metadata is being added to class metadata at 'Export'

        for collection, package in queue.items():
            # ensure dir exists and create the full file path
            fdp = DEFAULT_PROCESSING_OUTPUT_DIR / package['dirname']
            os.makedirs(fdp, exist_ok=True)

            fp = os.path.join(fdp, package['filename'])

            self._export(type=type, path=fp, final_df=self.__dict__[self.alias_map[collection]])
            self.metadata['Export']['Successful Exports'] += 1

            logger.info(f"Exported {collection} data to {fp}")

        return
    
    def _get_generic_export_queue(self) -> dict[str, str|pd.DataFrame|bool]:
        """ Formats a generic package containing default export data """

        default_output_dirs = CONFIG_DATA['default_output_directory_names']
        assert all(collection_alias in default_output_dirs for collection_alias in self.alias_map.keys()), f'Invalid config setup -> collection aliase values specified in data_aliases does not match the keys in default_output_directory_names. Collection aliases: {self.alias_map.keys()} | default output dirname keys: {default_output_dirs.keys()}'

        def_filename = get_current_time('FILE_DATE')

        export_package = {}
        for collection_alias, df_key in self.alias_map.items():
            export_package[collection_alias] = {
                'data': self.__dict__[df_key],
                'dirname': default_output_dirs[collection_alias],
                'filename': def_filename,
                'export': False
            }

        return export_package
    
    @staticmethod
    def _build_export_queue(package:dict[str,dict], all_data:bool, collections:tuple[str]|None, output_dir_map:dict|None, output_file_map:dict|None) -> tuple[dict[str, dict], dict]:
        """  Aggregegates necessary data (output locations) to export data 
        
        Returns 
        -------
            A tuple containing the export queue and metadata as dictionaries, respectively
        
        """
        intermediate_pkg = package.copy()
        # set which collections are valid exports
        if all_data:
            collections = tuple(intermediate_pkg.keys())
        # modify package in place, marking all collections as valid exports
        for alias, data in intermediate_pkg.items():
            data['export'] = True if alias in collections else False

        # will keep modular and separate for now but NOTE the two below loops could probably be combined in some way
        if output_dir_map:
            assert all(collection in output_dir_map for collection in collections), f'output_dir_map contains invalid collection aliases. Ensure all keys are in {intermediate_pkg.keys()}'
            for collection in output_dir_map:
                intermediate_pkg[collection]['dirname'] = output_dir_map[collection]
        
        if output_file_map:
            assert all(collection in output_file_map for collection in collections), f'output_file_map contains invalid collection aliases. Ensure all keys are in {intermediate_pkg.keys()}'
            for collection in output_file_map:
                intermediate_pkg[collection]['filename'] = output_file_map[collection]

        # remove any collections that are not marked for export
        final_queue = {collection:package for collection, package in intermediate_pkg.items() if package['export']}

        metadata = {
                'Queue Total': len(final_queue),
                'Collections': [collection for collection in final_queue],
                'Overridden DirNames': list(output_dir_map.keys()) if output_dir_map else None,
                'Overridden FileNames': list(output_file_map.keys()) if output_file_map else None,
                'Successful Exports': 0
            }
        
        return final_queue, metadata

        
    @staticmethod
    def _export(type:str, path:str, final_df:pd.DataFrame):
        if type == 'csv':
            final_df.to_csv(f'{path}.csv', index=False)
        elif type == 'pickle':

            final_df.to_pickle(f'{path}.pkl')
        else:
            raise ValueError(f'Unsupported export type: {type}')
        return
        

            
            


    # @@@@@@@@@@@@@@@@@@@ EXTRACTION/FORMATTING VALIDATION TESTS @@@@@@@@@@@@@@@@@@@
    # NOTE will break once new table size parameter is fully implemented 
    def validate_ba_to_bf_difference(self):
        """ Ensures length difference between box purchases and flavors dataframes is as expected.

        Length difference should be correlated with the number of costco packs processed
        
        """
        box_purchases:pd.DataFrame = self.box_purchases_df.copy()
        boxes_flavors:pd.DataFrame = self.box_flavors_df.copy()

        n_costco_packs = len(box_purchases[box_purchases['location']=='CCO'])

        length_diff = len(boxes_flavors) - len(box_purchases)

        assert (length_diff / 2) == n_costco_packs, f'Unexpected length difference between box_purchases and boxes_flavors dataframes.\n{n_costco_packs = } | {length_diff = }'
        
        logger.info('Passed BA/BF DF length difference validation')
        return


        
    # --------- RESULTS REVIEW AND VERIFICATION METHODS --------- #

    def display_run_stats(self):
        header = 'Pre Processing Results:'
        
        print(
            f'\n{header:10s}',
            f'Total Processed Purchases: {len(self.box_purchases_df)} | ',
            f'Total Processed Flavors: {len(self.box_flavors_df)} | ',
            f'Total Processed Cans: {len(self.can_data_df)}', end='\n\n'
        )

        # cavm_status, cavm_message = self.verify_box_can_count_manual()
        # print(f'\nCan Amount Verification : {cavm_status}\n{cavm_message}')
        return
        
    def verify_box_can_count_manual(self) -> tuple[str,str]:
        """ Manually calculates the expected can amount given the type of box (regular, costco, walgreens, etc) """
        expected_can_amount = 0
        for box in self.box_all_collection:
            if self.box_all_collection[box].location == 'CCO':
                expected_can_amount += 24
            elif self.box_all_collection[box].location == 'WLG':
                expected_can_amount += 6
            else:
                expected_can_amount += 8

        

        if expected_can_amount == len(self.can_data_collection):
            status = 'PASS'
            message =  f'The number of cans processed matches the expected number of cans to process.'
        else:
            status = 'FAIL'
            message = f'The number of cans processed does not match the expected number of cans to process.\n{expected_can_amount = }'
            difference = expected_can_amount - len(self.can_data_collection)
            if difference % 8 == 0:
                potential_empty_boxes = self._get_potential_empty_boxes()
                status = 'UNCERTAIN'
                message += f'\nMismatch is probably due to {int(difference/8)} empty box(es) in the box dataset. \n{potential_empty_boxes = }'

        return status, message
        

    