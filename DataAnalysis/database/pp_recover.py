import pandas as pd
import numpy as np
import os
import re
import datetime
import io
from typing import Literal, TypeAlias

from .. import logging, generate
from ..config import SPREADSHEET_DIR, DB_CONFIG_DIR
from .utils import _extract_format_db_config

logger = logging.getLogger('standard')

DATE_FORMAT = '%m/%d/%Y'
FILE_DATE_FORMAT = '%m%d%Y'
VERSION = datetime.datetime.now().strftime(FILE_DATE_FORMAT)

BOX_ALL_EXPORT = os.path.join(SPREADSHEET_DIR, 'processed', 'all_box_data', f'{VERSION}.csv')
BOX_FLAVOR_EXPORT = os.path.join(SPREADSHEET_DIR, 'processed', 'box_by_flavor', f'{VERSION}.csv')
CAN_EXPORT = os.path.join(SPREADSHEET_DIR, 'processed', 'all_can_data', f'{VERSION}.csv')

os.makedirs(BOX_ALL_EXPORT, exist_ok=True)
os.makedirs(BOX_FLAVOR_EXPORT, exist_ok=True)
os.makedirs(CAN_EXPORT, exist_ok=True)

DB_DATA_DIR = r'C:\Users\tmalo\Desktop\GitHub\LCT\DataAnalysis\spreadsheets'

HEADERS = {
    'BoxA': ['id', 'base_og_id', 'purchase_date', 'price', 'location'],
    'BoxF': ['id', 'box_id', 'flavor', 'start_date', 'finish_date', 'has_cans'],
    'Can': ['id', 'box_id', 'initial_mass', 'initial_volume', 'final_mass', 'final_volume', 'finish_status']
}



class PreProcessor:
    """Base utility class for pre-processing data using pandas DataFrames."""

    def __init__(self):
        self.id_map:dict[str, dict[Literal['purchase id', 'flavor id'], str]] = {} # maps og_id to purchase and flavor ids

        # transient tracking attributes
        self.extracted_box_data:list[pd.DataFrame] = []
        self.extracted_can_data:dict[str, pd.DataFrame] = {}

        self.box_all_df = pd.DataFrame(columns=HEADERS['BoxA'])
        self.box_flavor_df = pd.DataFrame(columns=HEADERS['BoxF'])
        self.can_data_df = pd.DataFrame(columns=HEADERS['Can'])


        self.source_data_attributes:dict[Literal['boxes_all','boxes_flavor','can_data'], dict[Literal['data', 'headers', 'output_file'], dict|list]] = {
            'boxes_all': {
                'data': None,
                'headers': HEADERS['BoxA'],
                'output_file': BOX_ALL_EXPORT
            },
            'boxes_flavor': {
                'data': None,
                'headers': HEADERS['BoxF'],
                'output_file': BOX_FLAVOR_EXPORT
            },
            'can_data': {
                'data': None,
                'headers': HEADERS['Can'],
                'output_file': CAN_EXPORT
            }
        }

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
        self.source_data_attributes['boxes_all']['data'] = box_all_df


        # extract/format box_flavor data
        bf_df = box_data_df[['og_id', 'base_og_id', 'flavor', 'start_date', 'finish_date']] # TODO add column extraction to dedicated CONSTANT file

        # get box id from ba df based on poi, inserted at beginning
        bf_df.insert(0, 'box_id', bf_df['base_og_id'].apply(lambda x:self._get_box_id(df= box_all_df, base_original_box_id=x)))
        bf_df.insert(0, 'id', [generate(size=7) for _ in range(len(bf_df))])
        # bf_df['has_cans'] = False # TODO add column to headers for DB insertion
        bf_df.insert(len(bf_df.columns), 'has_cans', False) # TODO add column to headers for DB insertion

        # create id map before dropping unnecessary cols for box flavor DF
        ids_to_delete = ['og_id', 'base_og_id']
        self._create_id_map(bf_df[ids_to_delete+['id']], box_all_df[['id', 'base_og_id']])

        box_flavor_df = bf_df.drop(columns=ids_to_delete)

        self.resolve_dtypes(box_flavor_df)
        self.source_data_attributes['boxes_flavor']['data'] = box_flavor_df

        logger.info(f'Successfully created and saved box dataframes')
        self.validate_ba_to_bf_difference() # NOTE still deciding if I want to call this here

        return

    def _process_can_data(self):
        """
        
        """
        all_flavors:pd.DataFrame = self.source_data_attributes['boxes_flavor']['data']

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

        
        self.source_data_attributes['can_data']['data'] = pd.concat(processed_data, ignore_index=True)
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
        hdr_map = dict(zip(hdrs_to_rename[1:], HEADERS['Can'][2:]))
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

    def export_to_file(self) -> None:
        """Exports the DataFrames to CSV files."""
        self.box_all_df.to_csv(BOX_ALL_EXPORT, index=False)
        self.box_flavor_df.to_csv(BOX_FLAVOR_EXPORT, index=False)
        self.can_data_df.to_csv(CAN_EXPORT, index=False)
        logger.info("Exported all data to CSV files.")




    # @@@@@@@@@@@@@@@@@@@ EXTRACTION/FORMATTING VALIDATION TESTS @@@@@@@@@@@@@@@@@@@
    def validate_ba_to_bf_difference(self):
        """ Ensures length difference between boxes_all and boxes_flavor dataframes is as expected.

        Length difference should be correlated with the number of costco packs processed
        
        """
        boxes_all:pd.DataFrame = self.source_data_attributes['boxes_all']['data']
        boxes_flavor:pd.DataFrame = self.source_data_attributes['boxes_flavor']['data']

        n_costco_packs = len(boxes_all[boxes_all['location']=='CCO'])

        length_diff = len(boxes_flavor) - len(boxes_all)

        assert (length_diff / 2) == n_costco_packs, f'Unexpected length difference between boxes_all and boxes_flavor dataframes.\n{n_costco_packs = } | {length_diff = }'
        
        logger.info('Passed BA/BF DF length difference validation')
        return




    