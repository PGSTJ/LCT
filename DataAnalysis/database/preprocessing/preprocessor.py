"""


"""

from .config import (TypeAlias, Literal, pd, traceback, re, io, csv, os,
                     HEADERS, BOX_ALL_EXPORT, BOX_FLAVOR_EXPORT, CAN_EXPORT, CSV_DATA_DIR, MD_DATA_DIR,
                     logger)

from .objects import (BoxAllData, BoxCanBase, BoxFlavorData, CanData)


DataCollectionMetadataType: TypeAlias = dict[Literal['data', 'headers', 'output_file'], dict|list]
DataCollectionType: TypeAlias = dict[Literal['boxes_all','boxes_flavor','can_data'], DataCollectionMetadataType]

class PreProcessor():
    """ Base utility class for pre-processing data 
    
    Parameters:
        box_all_collection (dict {pure_og_id: BoxAllData}) : 
            Holds data pertaining to boxes_all DB table.
        
        box_flavor_collection (list[BoxFlavorData]) : 
            Holds data pertaining to boxes_flavor DB table.
        
        can_data_collection (list[CanData]) : 
            Holds data pertaining to can_data DB table.
    
    """

    

    def __init__(self, box_source_path:str=None, can_source_path:str=None):
        self.can_source_path = can_source_path
        self.box_source_path = box_source_path

        self.box_all_collection:dict[str, BoxAllData] = {}
        self.box_flavor_collection:list[BoxFlavorData] = []
        self.can_data_collection:list[CanData] = []

        self.source_data_attributes:dict[Literal['boxes_all','boxes_flavor','can_data'], dict[Literal['data', 'headers', 'output_file'], dict|list]] = {
            'boxes_all': {
                'data': self.box_all_collection,
                'headers': HEADERS['BoxA'],
                'output_file': BOX_ALL_EXPORT
            },
            'boxes_flavor': {
                'data': self.box_flavor_collection,
                'headers': HEADERS['BoxF'],
                'output_file': BOX_FLAVOR_EXPORT
            },
            'can_data': {
                'data': self.can_data_collection,
                'headers': HEADERS['Can'],
                'output_file': CAN_EXPORT
            }
        }


        
    # --------- PROCESSING UTILS --------- #

    def _update_box_id(self, can_data:list[dict[str,str]], original_box_id:str) -> list[dict[str,str]]:
        """ Updates can id with formatted id and adds column for generated box id, mapped from the original box id """
        generated_bid = self._get_box_id(original_box_id)
        for cans in can_data:
            can_num = cans['can_id']
            cans['box_id'] = generated_bid
            cans['id'] = f'{can_num}.{generated_bid}'
        return can_data

    def _get_box_id(self, purified_original_box_id:str) -> str | None:
        """Gets generated box id from collection with pure original box id"""
        try:
            return self.box_all_collection[purified_original_box_id].id
        except KeyError:
            logger.error(f'{purified_original_box_id} does not exist in the box_all collection')
        return None

    @staticmethod
    def _get_pure_ogid(full_og_id:str) -> str:
        """ "Pure" to account for costco packs with an extra number denoting which flavor in the pack (i.e. 1-3) """
        match_ogid = re.match(r"(\d+)([A-Za-z]+)", full_og_id) 
        number, flavor = match_ogid.groups()
        return number + flavor  

    def _get_all_data_raw(self) -> dict[str, dict[str, BoxAllData]|list[BoxFlavorData]|list[CanData]]:
        return {data: self.__dict__[data] for data in self.__dict__ if 'collection' in data}

    def _collect_box_data(self, pure_og_bid:str, box_data:dict[str,str], last_changed_box:BoxAllData=None) -> BoxAllData|None:
        """ Groups box data into one collection as objects """
        box:BoxAllData = last_changed_box
        if pure_og_bid not in self.box_all_collection:
            box = BoxAllData(box_data)
            logger.info(f'Created BoxA - og_id: {box.og_id} | bid: {box.id}')
            logger.debug(f'\tdata: {box_data}')
            self.box_all_collection[pure_og_bid] = box

        try:
            box_f = BoxFlavorData(box_data, box.id)
            self.box_flavor_collection.append(box_f)
            logger.info(f'created BoxF - bid: {box_f.box_id} | bfid: {box_f.id}')
            logger.debug(f'\tbfid {box_f.id} data: {box_f.__dict__}')
        except NameError:
            # TODO convert to logger?
            traceback.print_exc()
            logger.error(f'Box object has not been created for pure og id: {pure_og_bid}. Ensure this box is created before attempting to create a box-flavor association.')

        return box if box else None
        
    def _collect_can_data(self, can_data:list[dict[str,str]], pure_og_bid:str) -> None:
        """ Groups can data into one collection as objects """
        if can_data is None:
            return
        updated_cd = self._update_box_id(can_data, pure_og_bid)
        obj_ver_upd_cd = [CanData(data) for data in updated_cd]
        logger.debug(f'Created {len(obj_ver_upd_cd)} CanData objects for box {pure_og_bid}')
        self._update_saved_can_data(obj_ver_upd_cd, override=False)
        return

    def _update_saved_box_data(self, updated_box_all:dict[str,BoxAllData], updated_box_flavors:list[BoxFlavorData], list_override:bool=True):
        """ Updates the saved box collections with new data, either by overriding or appending if a list """
        self.box_all_collection = updated_box_all
        if list_override:
            self.box_flavor_collection = updated_box_flavors
            return
        self.box_flavor_collection += updated_box_flavors
        return

    def _update_saved_can_data(self, updated_cans:list[CanData], override:bool=True):
        """ Update the saved can collection with new data, either by overriding or appending """
        if override:
            self.can_data_collection = updated_cans
            return
        self.can_data_collection += updated_cans
        return


    # --------- RESULTS EXPORT METHODS --------- #
    
    def export_data(
            self, 
            data_collections:list[Literal['ba','bf','cd']]=None, 
            output_files:dict[Literal['ba','bf','cd'], str]=None,
            override_existing_content:list[Literal['ba','bf','cd', '*']]|bool=False
            ):
        """ Writes data to output files for DB processing.

        This will export all data collection types to the default output files defined in
        the ```self.source_data_attributes``` metadata property without overriding any data currently
        in the specified file (specific file will change depending on the day due to the VERSION 
        constant). 
        
        For a complete and clean override of the current default file, only the first instance of the pre
        processing pipeline (PreProcessorCSV or PreProcessorMD) calling this method should include an 
        override_existing_content parameter. For example:

            ```
            pp_csv = PreProcessCSV()
            pp_md = PreProcessesMD()

            pp_csv.run_pre_processing()
            pp_csv.display_run_stats()
            pp_md.run_pre_processing()
            pp_md.display_run_stats()

            pp_csv.export_data(override_existing_output=['*']) # only this export_data() call supplies an override_existing_output parameter
            pp_md.export_data()

            ```
        
        Parameters:
            data_collections : list[Literal['ba','bf','cd']], default=None
                defines abbreviations of desired collections to export, which maps  
                the data collection type to the default output file and its header.
        
            output_files : dict[Literal['ba','bf','cd'], str], default=None
                overrides default output file destinations of specified DataCollectionType via 
                alias. Default is None, meaning default file locations will be used.

            override_existing : list[Literal['ba','bf','cd', '*']] | bool, default=False
                overrides contents of specified DataCollectionType via alias, or override all 
                with the "override all" alias, "*". Default is False.

        """
        
        class_annotation = self._get_type_annotation()
        logger.info(f'Started exporting {class_annotation} data')

        # export modifier maps- override current/default export file contents/location
        content_override_map:list[str] = self._define_content_overriding(override_existing_content)
        override_output_dst_map:dict[str,str] = self._define_output_dst(output_files)

        # the actual data being processed
        queue:dict[str,DataCollectionType] = self._define_export_data(data_collections)

        for data_collection_type in queue:
            metadata = self.source_data_attributes[data_collection_type]

            content_override = True if data_collection_type in content_override_map else False
            # override output dst if specified, o/w use default
            metadata['output_file'] = override_output_dst_map[data_collection_type] if data_collection_type in override_output_dst_map else self.source_data_attributes[data_collection_type]['output_file']
            
            formatted_data = self._format_export_data(metadata, override=content_override)

            # write to file based on formatted_data
            with open(metadata['output_file'], 'a') as fn:
                wtr = csv.DictWriter(fn, metadata['headers'], lineterminator='\n')
                wtr.writerows(formatted_data)
            
        logger.info(f'Finished exporting {class_annotation} data to {metadata['output_file']}')
        return

    def _define_content_overriding(self, data_collections_to_override:list[Literal['ba','bf','cd', '*']]|bool) -> list[str]:
        """ Formats content override instructions for specified data collections """
        if isinstance(data_collections_to_override, list):
            if '*' in data_collections_to_override:
                return [dct for dct in self.source_data_attributes]
            else: 
                return [self._convert_dc_abbreviation(abbr) for abbr in data_collections_to_override]
        return []

    def _define_output_dst(self, op_files:dict[str, str]) -> dict[str,str]:
        """ Formats alternative output files for specified data collections """
        if op_files:
            return {self._convert_dc_abbreviation(abbr):op_files[abbr] for abbr in op_files}
        return {}
    
    def _define_export_data(self, collections:list[str]|None) -> dict[str,DataCollectionType]:
        """ Formats specified collections for exporting, or returns a copy of entire metadata map """
        if collections:
            dc_dbt_name:list[str] = [self._convert_dc_abbreviation(abbr) for abbr in collections]
            ex_data = {data:self.source_data_attributes[data] for data in self.source_data_attributes if data in dc_dbt_name}
        else:
            ex_data = self.source_data_attributes.copy()

        return ex_data

    def _format_export_data(self, meta_data:dict[Literal['data', 'headers', 'output_file'], dict|list], override:bool=False) -> list[dict[str,str]]:
        self._format_file(meta_data['output_file'], meta_data['headers'], override=override)

        # access data contents differently depending on collection type, can only either be a dict or list
        data:dict[str, BoxAllData] | list[BoxFlavorData|CanData] = meta_data['data']
        if isinstance(data, dict):
            return [data[pure_ogid].write_export_format() for pure_ogid in data]
        elif isinstance(data, list):
            return [info.write_export_format() for info in data]
        else:
            err_package = {
                'processor type':self._get_type_annotation(),
                'collection name': os.path.basename(meta_data["output_file"])[:-13],
                'collection type': type(data),
            }

            logger.error(f'CANNOT FORMAT EXPORT DATA FOR {self._get_type_annotation()} DUE TO COLLECTION OF TYPE {type(data)}')
            raise ValueError(f'Incompatible data collection type. Must be either a dict or a list. \n Collection: {err_package['collection name']} | Current Type: {err_package['collection type']}')
        # TODO some error due to foreign data collection type (meaning it was altered somewhere)


    @staticmethod
    def _format_file(output_file_path:str, file_header:list[str], default_mode:str='w', override:bool=False) -> bool:
        """ Validates output file existence and prepares the file if needed """
        if os.path.isfile(output_file_path) and not override:
            default_mode = 'r+'
                
        with open(output_file_path, default_mode) as fn:
            wtr = csv.DictWriter(fn, file_header, lineterminator='\n')
            contents = [data for data in csv.DictReader(fn, file_header)] if default_mode != 'w' else []
        
            if len(contents) > 1:
                return True
            # TODO depending on file replacing algorithm will add reset mechanism
            
            wtr.writeheader()
        return True
    
    @staticmethod
    def _convert_dc_abbreviation(abbreviation:str) -> str:
        """ Converts data collection abbreviation into full name for logging purposes """
        map = {
            'ba': 'boxes_all',
            'bf': 'boxes_flavor',
            'cd': 'can_data',
        }

        if abbreviation not in map:
            raise KeyError(f'This abbreviation: {abbreviation} has not been registered as a data collection type.')
        
        return map[abbreviation]
        
    def _get_type_annotation(self) -> str:
        """ Returns CSV or MD for status updates """
        if isinstance(self, PreProcessCSV):
            return 'CSV'
        elif isinstance(self, PreProcessesMD):
            return 'MD'

        
    # --------- RESULTS REVIEW AND VERIFICATION METHODS --------- #

    def display_run_stats(self):
        if isinstance(self, PreProcessCSV):
            header = 'CSV Processing Results:'
        elif isinstance(self, PreProcessesMD):
            header = 'MD Processing Results:'
        
        print(
            f'\n{header:10s}',
            f'Number of boxes/packs: {len(self.box_all_collection)} | ',
            f'Number of flavors: {len(self.box_flavor_collection)} | ',
            f'Number of cans: {len(self.can_data_collection)}',
        )

        cavm_status, cavm_message = self.verify_box_can_count_manual()
        print(f'\nCan Amount Verification : {cavm_status}\n{cavm_message}')
        
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
        
    def _get_potential_empty_boxes(self) -> list[str]:
        """ Quickly queries for box_ids not represented within the can dataset """
        represented_boxes_in_cans = {can.box_id for can in self.can_data_collection}
        repesented_boxes_in_flavor = {box.box_id for box in self.box_flavor_collection}

        cans_missing_boxes = [self.box_all_collection[box].og_id for box in self.box_all_collection if self.box_all_collection[box].id not in represented_boxes_in_cans]
        flavors_missing_boxes = [self.box_all_collection[box].og_id for box in self.box_all_collection if self.box_all_collection[box].id not in repesented_boxes_in_flavor]
        
        if flavors_missing_boxes == cans_missing_boxes:
            return cans_missing_boxes
        else:
            return cans_missing_boxes + flavors_missing_boxes

    
        


            


class PreProcessCSV(PreProcessor):
    """ Pre Processes CSV files (typically older saved data) 
    
    Default source directories (can and box) are local in ./spreadsheets/CSV
    
    """
    def __init__(self, box_source_path = None, can_source_path = None):

        box_source:str = os.path.join(CSV_DATA_DIR, 'box_data.csv') if box_source_path is None else box_source_path
        can_source:str = os.path.join(CSV_DATA_DIR, 'can_data_by_box') if can_source_path is None else can_source_path
        
        super().__init__(box_source, can_source)

    def run_pre_processing(self) -> tuple[dict[str,BoxAllData], list[BoxFlavorData], list[CanData]]:
        """ EXECUTION FUNCTION FOR PROCESSING DATA IN CSVs (currently documented as 'prior data') """
        logger.info('Started processing CSV data')
        self._process_box_data()    
        self._process_can_data()
        logger.info(f'Finished processing CSV data')
        return 

    def _process_box_data(self):
        """ EXECUTION FUNCTION for processing box data within CSVs """
        box_data = self._read_box_data()

        box: BoxAllData = None
        for data in box_data:
            pure_og_id = self._get_pure_ogid(data['og_id'])

            upd_box = self._collect_box_data(pure_og_id, data, box)
            box = upd_box if upd_box is not None else box
        
        logger.info(f'Finished processing box data from CSVs')
        return

    def _read_box_data(self) -> list[dict[str,str]]:
        """Read box data CSV file"""
        with open(self.box_source_path, 'r') as fn:
            fn.readline()
            return [info for info in csv.DictReader(fn, HEADERS['BoxDS'])]
        
    def _process_can_data(self):
        """EXECUTION FUNCTION for processing can data within CSVs"""
        for file in os.listdir(self.can_source_path):
            p_og_bid, can_data = self._read_can_data(os.path.join(self.can_source_path, file))
            self._collect_can_data(can_data, p_og_bid)

        logger.info(f'Finished processing can data from CSVs')
        return 

    def _read_can_data(self, file_path:str) -> tuple[str, list[dict[str, str]]]:
        """Read and format can data from and for CSV"""
        original_box_id_purified = self._get_pure_ogid(os.path.basename(file_path)[:-4])

        # also overrides file headers with DB compatible versions
        adjusted_header = HEADERS['Can'][:-2]
        adjusted_header.remove('box_id')
        
        with open(file_path, 'r') as fn:
            fn.readline()
            can_data = [info for info in csv.DictReader(fn, adjusted_header)]
                
        return original_box_id_purified, can_data
    

class PreProcessesMD(PreProcessor):
    """ Pre Processes MD files (direct Notion exports) 
    
    Currently only supports passing in one directory of MD files containing both
    can and box source data. This directory should be passed as the "box_source_path"
    parameter.
    
    """
    def __init__(self, box_source_path = None, can_source_path = None):
        dir = os.path.join(MD_DATA_DIR, 'can_data_by_box') if box_source_path is None else box_source_path
        can_source = None
        
        super().__init__(dir, can_source)

        

    def run_pre_processing(self) -> None:
        """ EXECUTION FUNCTION FOR PROCESSING DATA IN CSVs (currently documented as 'current data') """
        logger.info('Started processing MD data')
        box:BoxAllData | None = None
        
        for md_file in os.listdir(self.box_source_path):
            pure_og_id = self._get_pure_ogid(md_file.split(' ')[0])
            props, table_data = self._read_markdown_data(os.path.join(self.box_source_path, md_file))
            logger.debug(f'\nExtracted property data: {props}')

            # extract/collect box data
            formatted_box_properties = self._box_header_format_converter(props)
            formatted_box_properties['og_id'] = pure_og_id
            upd_box = self._collect_box_data(pure_og_id, formatted_box_properties, box)
            box = upd_box if upd_box is not None else box

            # extract/collect can data for this box
            formatted_can_data = self._can_format_converter(table_data) # Needs box data to be created as objects for ID
            self._collect_can_data(formatted_can_data, pure_og_id)

        logger.info(f'Finished processing MD data')
        return 
        
    @staticmethod
    def _read_markdown_data(file_path:str) -> tuple[dict[str,str], pd.DataFrame|None]:
        """Parses exported Notion Page markdown files into properties and description data; the latter of
        which is essentially table data given how LCT Notion Pages are designed."""
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

    def _can_format_converter(self, data:pd.DataFrame) -> list[dict,str,str] | None:
        """ Converts from MD extracted can data to current format of can data """
        if data is None:
            return
        
        format_dict = data.transpose().to_dict()
        format_dict.pop(0)
        # list indices (rows) are dicts (len(dict) = # of columns) representing the can data
        collected:list[dict[str,str]] = [format_dict[data] for data in format_dict]

        adjusted_header: list[str] = self.source_data_attributes['can_data']['headers'][:-2]
        adjusted_header.remove('box_id')

        # adjusts keys to match current header format
        final = []
        for can in collected:
            header_values = [can[caninfo] if can[caninfo] != '' else 'NA' for caninfo in can] # list of row values with empty spaces converted to 'NA'

            if len(header_values) != len(adjusted_header):
                raise ValueError(f'Amount of headers ({len(adjusted_header)}) does not match the amount of headers in the extracted data ({len(header_values)})\n{adjusted_header = }\n')
            final.append(dict(zip(adjusted_header, header_values)))

        return final