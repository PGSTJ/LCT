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
        self.extracted_box_data:list[pd.DataFrame] = []


        # transient tracking attributes
        self.processed_pure_ids:list[str] = []

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
            box_data_df, can_data_dfs = self.extract_csv_data(csv_data_dir)
            self.extracted_box_data.append(box_data_df)

        if md_data_dir:
            box_data_df, can_data_dfs = self.extract_md_data(md_data_dir)
            self.extracted_box_data.append(box_data_df)
        
        all_box_data = pd.concat(self.extracted_box_data, ignore_index=True)

        self._process_box_data(all_box_data)
        return

        for og_id, can_data_df in can_data_dfs.items():
            self._process_can_data(can_data_df)

    
    def extract_csv_data(self, data_dir:str) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """ Extract box/can data from CSV files
        
        Returns
        -------
            Tuple of box and can data, where box data is a Dataframe and can data is a DataframeGroupBy collection
        
        """
        box_data_path = os.path.join(data_dir, 'box_data.csv')
        can_data_path = os.path.join(data_dir, 'can_data_by_box')

        box_data_df = pd.read_csv(box_data_path)
        norm_box_data_df = self._normalize_box_df(box_data_df, type='csv')


        can_data_dfs = {file[:-4]:pd.read_csv(os.path.join(can_data_path, file)) for file in os.listdir(can_data_path)}

        return norm_box_data_df, can_data_dfs


    def extract_md_data(self, md_data_dir:str) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
        """ Extract box/can data from MD files
        

        
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

            can_data_dict[og_id] = table_data

            # upd_box = self._collect_box_data(base_og_id, formatted_box_properties, box)
            # box = upd_box if upd_box is not None else box

            # # extract/collect can data for this box
            # formatted_can_data = self._can_format_converter(table_data) # Needs box data to be created as objects for ID
            # self._collect_can_data(formatted_can_data, base_og_id)

        box_df = pd.DataFrame(box_data_dicts)
        norm_box_df = self._normalize_box_df(box_df, 'md')



        return norm_box_df, can_data_dict



    def _process_box_data(self, box_data_df:pd.DataFrame):
        
        # extract/format box_all data
        all_badf = []
        
        for base_og_id, df in box_data_df.groupby('base_og_id'):        
            intermediate_ba_df = df[['purchase_date', 'price', 'location']].drop_duplicates() # TODO add column extraction to dedicated CONSTANT file
            intermediate_ba_df.insert(0, 'base_og_id', base_og_id)
            intermediate_ba_df.insert(0, 'id', generate(size=7))
            all_badf.append(intermediate_ba_df)
            
        box_all_df = pd.concat(all_badf, ignore_index=True)

        # extract/format box_flavor data
        bf_df = box_data_df[['base_og_id', 'flavor', 'start_date', 'finish_date']] # TODO add column extraction to dedicated CONSTANT file

        # get box id from ba df based on poi, inserted at beginning
        bf_df.insert(0, 'box_id', bf_df['base_og_id'].apply(lambda x:self._get_box_id(df=box_all_df, purified_original_box_id=x)))
        bf_df.insert(0, 'id', [generate(size=7) for _ in range(len(bf_df))])
        # bf_df['has_cans'] = False # TODO add column to headers for DB insertion
        bf_df.insert(len(bf_df.columns), 'has_cans', False) # TODO add column to headers for DB insertion

        box_flavor_df = bf_df.drop(columns=['base_og_id'])

        self.resolve_dtypes(box_all_df)
        self.resolve_dtypes(box_flavor_df)

        self.source_data_attributes['boxes_all']['data'] = box_all_df
        self.source_data_attributes['boxes_flavor']['data'] = box_flavor_df

        logger.info(f'Successfully created and saved box dataframes')
        self.validate_ba_to_bf_difference() # NOTE still deciding if I want to call this here

        return


    def _process_can_data(self, base_og_id:str, can_data_df:pd.DataFrame):
        """
        
        """



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

    @staticmethod
    def resolve_dtypes(df:pd.DataFrame):

        for col in df.columns:
            if col == 'price':
                df['price'] = df['price'].astype('float32')
            elif 'date' in col:    
                df[col] = pd.to_datetime(df[col])
            elif df[col].dtype == 'bool':
                continue
            else:
                df[col] = df[col].astype('string')
        return df




    @staticmethod
    def _get_base_ogid(full_og_id: str) -> str:
        """Extracts the pure original ID from the full ID."""
        match_ogid = re.match(r"(\d+)([A-Za-z]+)", full_og_id)
        number, flavor = match_ogid.groups()
        return number + flavor

    def _update_box_id(self, can_data: pd.DataFrame, original_box_id: str) -> pd.DataFrame:
        """Updates can IDs with formatted IDs and adds a column for generated box IDs."""
        generated_bid = self._get_box_id(original_box_id)
        can_data['box_id'] = generated_bid
        can_data['id'] = can_data['id'].apply(lambda x: f"{x}.{generated_bid}.{generate(size=4)}")
        return can_data

    def _get_box_id(self, df:pd.DataFrame, purified_original_box_id: str) -> str | None:
        """Gets the generated box ID from the collection."""
        try:
            return df.loc[df['base_og_id'] == purified_original_box_id, 'id'].values[0]
        except IndexError:
            logger.error(f"{purified_original_box_id} does not exist in the box_all collection")
            return None
        

    def _process_can_data_ddd(self) -> None:
        """Processes can data from CSV files."""
        for file in os.listdir(self.can_source_path):
            file_path = os.path.join(self.can_source_path, file)
            can_data = pd.read_csv(file_path)
            base_og_id = self._get_base_ogid(os.path.basename(file)[:-4])
            self._collect_can_data(can_data, base_og_id)

    def _collect_box_data(self, pure_og_bid: str, box_data: dict[str, str]) -> None:
        """Adds box data to the box_all_df and box_flavor_df DataFrames."""
        if pure_og_bid not in self.processed_pure_ids:
            self.processed_pure_ids.append(pure_og_bid)
            box_data.update({'id':generate(size=7)})
            self.box_all_df = pd.concat([self.box_all_df, pd.DataFrame([box_data])], ignore_index=True)
            logger.info(f"Created BoxA - og_id: {box_data['og_id']} | id: {box_data['id']}")

        box_flavor_data = {
            'id': generate(size=7),
            'box_id': self._get_box_id(pure_og_bid),
            'flavor': box_data.get('flavor', ''),
            'start_date': box_data.get('start_date', ''),
            'finish_date': box_data.get('finish_date', '')
        }
        self.box_flavor_df = pd.concat([self.box_flavor_df, pd.DataFrame([box_flavor_data])], ignore_index=True)
        logger.info(f"Created BoxF - box_id: {box_flavor_data['box_id']} | id: {box_flavor_data['id']}")

        return

    def _collect_can_data(self, can_data: pd.DataFrame, pure_og_bid: str) -> None:
        """Adds can data to the can_data_df DataFrame."""
        if can_data.empty:
            logger.warning(f"No can data passed for {pure_og_bid}")
            return

        updated_can_data = self._update_box_id(can_data, pure_og_bid)
        self.can_data_df = pd.concat([self.can_data_df, updated_can_data], ignore_index=True)
        logger.debug(f"Created {len(updated_can_data)} CanData objects for box {pure_og_bid}")

        return

    def export_to_file(self) -> None:
        """Exports the DataFrames to CSV files."""
        self.box_all_df.to_csv(BOX_ALL_EXPORT, index=False)
        self.box_flavor_df.to_csv(BOX_FLAVOR_EXPORT, index=False)
        self.can_data_df.to_csv(CAN_EXPORT, index=False)
        logger.info("Exported all data to CSV files.")





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

    def _can_format_converter(self, data:pd.DataFrame) -> list[dict,str,str] | None:
        """ Converts from MD extracted can data to current format of can data """
        if data is None:
            return
        
        format_dict = data.transpose().to_dict()
        format_dict.pop(0)
        # list indices (rows) are dicts (len(dict) = # of columns) representing the can data
        collected:list[dict[str,str]] = [format_dict[data] for data in format_dict]

        adjusted_header: list[str] = self.source_data_attributes['can_data']['headers']
        if 'box_id' in adjusted_header:
            adjusted_header.remove('box_id')

        # adjusts keys to match current header format
        final = []
        for can in collected:
            header_values = [can[caninfo] if can[caninfo] != '' else 'NA' for caninfo in can] # list of row values with empty spaces converted to 'NA'

            if len(header_values) != len(adjusted_header):
                raise ValueError(f'Amount of headers ({len(adjusted_header)}) does not match the amount of headers in the extracted data ({len(header_values)})\n{adjusted_header = }\n')
            final.append(dict(zip(adjusted_header, header_values)))

        return final
    


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




    