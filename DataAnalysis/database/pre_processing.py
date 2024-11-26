"""Pre-processes data to upload to DB, currently divided between prior (2023) and current (2024) datasets.

Running this script will automatically override the existing can/box data if --two_step is set to false.

Run Script:
python DataAnalysis\database\pre_processing.py --two_step false

Dataset Sources:
    Prior Data -> DataAnalysis\spreadsheets\prior_data\
    Current Data -> DataAnalysis\spreadsheets\LCT_2024\
        * Both contain can_data_by box\ and box_data.csv

    
Output Sources:
    all_box_data.csv
    all_can_data.csv

"""

import csv
import logging
import os
import pandas as pd
import re
import io
import argparse
from typing import Literal
from nanoid import generate

# from . import logging, csv, os, pd, re, io

logger = logging.getLogger('standard')

SPREADSHEET_DIR = os.path.abspath(r'C:\Users\tmalo\Desktop\GitHub\LCT\DataAnalysis\spreadsheets')
PRIOR_DATA_DIR = os.path.join(SPREADSHEET_DIR, 'prior_data')
CURRENT_DATA_DIR = os.path.join(SPREADSHEET_DIR, 'current_data')

HEADERS = {
    "Can": [
        'Can',
        'Initial Mass',
        'Initial Volume',
        'Final Mass',
        'Final Volume',
        'Finished',
        'Box'
    ],
    "Box": [
        'bid',
        'flavor',
        'purchase_date',
        'price',
        'location',
        'started',
        'finished',
        'DV',
        'TTS'
    ]
}

BOX_EXPORT = os.path.join(SPREADSHEET_DIR, 'all_box_data.csv')
CAN_EXPORT = os.path.join(SPREADSHEET_DIR, 'all_can_data.csv')





class InvalidHeader(Exception):
    def __init__(self, output_path:str):
        self.message = f'Unable to determine header type from output path: {output_path}'
        super().__init__(self.message)

class MergerError(Exception):
    def __init__(self, box_output:str, can_output:str):
        self.message = f'Unable to merge files. Please check that the output files path to the \"_review\" CSVs.\n{box_output = }\n{can_output = }'
        super().__init__(self.message)

class MappingError(Exception):
    def __init__(self, *args):
        super().__init__(*args)





#######################################################################################################
###########                                                                                 ###########
###########                                                                                 ###########
###########                                   PRIOR DATA                                    ###########
###########                                                                                 ###########
###########                                                                                 ###########
#######################################################################################################


    




def process_can_data(can_data_dir:str, can_output_path:str) -> dict[str,str]:
    """Format and write can data to output CSV"""
    box_id_mapper = {}
    for file in os.listdir(can_data_dir):
        can_data = _read_can_data(os.path.join(can_data_dir, file))
        new_box_id = generate(size=7)
        
        updated_group, updated_can_data = _convert_update_box_id(box_id_mapper, can_data, can_data[0]['Box'], new_box_id)
        box_id_mapper = updated_group
        can_data = updated_can_data

        _write_data(can_output_path, can_data)
    print(f'Finished processing/storing prior can data from {can_data_dir} to {can_output_path}')
    return box_id_mapper

def _convert_update_box_id(id_mapper:dict[str,str], cans_in_box:list[dict[str,str]], old_id:str, new_id:str):
    """Converts box id, updates can list and id map"""
    id_mapper[old_id] = new_id
    for can in cans_in_box:
        can['Box'] = new_id
    return id_mapper, cans_in_box


def _read_can_data(file_path:str) -> list[dict[str, str]]:
    """Read and format can data from and for CSV"""
    box_id = os.path.basename(file_path)[:-4]
    with open(file_path, 'r') as fn:
        write_format = [info for info in csv.DictReader(fn, HEADERS['Can'])]
        for data in write_format:
            data['Box'] = box_id
        
        return write_format

def process_box_data(box_data_file:str, box_output_path:str, box_id_map:dict[str,str]):
    """Format and write box data to output CSV"""
    box_data = _read_box_data(box_data_file)

    for data in box_data:
        if data['bid'] in box_id_map:
            data['bid'] = box_id_map[data['bid']]

    _write_data(box_output_path, box_data)
    print(f'Finished processing/writing prior box data to {box_output_path}')
    
def _read_box_data(box_data_file:str):
    with open(box_data_file, 'r') as fn:
        fn.readline()
        write_format = [info for info in csv.DictReader(fn, HEADERS['Box'])]
        return write_format

def _write_data(output_path:str, data:list[dict[str,str]]):
    """Write can/box data to specified output file"""
    hdr_type = _determine_header(output_path)
    if hdr_type == None:
        raise InvalidHeader(output_path)
    
    hdr = HEADERS[hdr_type]

    with open(output_path, 'a') as fn:
        wtr = csv.DictWriter(fn, hdr, lineterminator='\n')
        wtr.writerows(data[1:])
    return

def _determine_header(output_path:str) -> str | None:
    file_name = os.path.basename(output_path)
    if 'can' in file_name:
        return 'Can'
    elif 'box' in file_name:
        return 'Box'
    else:
        return None



def process_prior_data(prior_data_dir:str, can_output:str, box_output:str):
    """Execution Function
    
    Output: all_can_data.csv

    """
    can_data_dir = os.path.join(prior_data_dir, 'can_data_by_box')
    box_data = os.path.join(prior_data_dir, 'box_data.csv')

    can_data = process_can_data(can_data_dir, can_output)
    process_box_data(box_data, box_output, can_data)
    
    logger.info(f'Finished combining all prior data from {prior_data_dir}')
    return




#######################################################################################################
###########                                                                                 ###########
###########                                                                                 ###########
###########                                 CURRENT DATA                                    ###########
###########                                                                                 ###########
###########                                                                                 ###########
#######################################################################################################




class BoxExportData():
    def __init__(self, data:dict[str,str]):
        self.uid:str = data['UID']
        self.flavor:str = data['Flavor']
        self.purchase_date:str = data['Purchased']
        try:
            self.price:float = float(data['Price'])
        except KeyError:
            self.price:str = 'NA'
        self.location:str = data['Location']
        try:
            self.start_date:str = data['Started']
        except KeyError:
            self.start_date:str = 'NA'
        try:
            self.finish_date:str = data['Finished']
        except KeyError:
            self.finish_date:str = 'NA'
        self.tracking:bool = data['Tracking']


    def _csv_export(self) -> tuple[dict[str, str], list[str]]:
        """formats data for CSV export-mainly for the master list"""
        return self.__dict__, [hdr for hdr in self.__dict__]
    


def process_current_data(current_data_dir:str, can_output_file:str, box_output_file:str):
    """Execution Function.
    
    Extracts current box and can data from Notion CSV and MD files, then exports into master CSVs.
    Output: 
    
    """

    can_data_dir = os.path.join(current_data_dir, 'can_data_by_box')
    box_data = os.path.join(current_data_dir, 'box_data.csv')

    for md_file in os.listdir(can_data_dir):
        props, table_data = _read_markdown_data(os.path.join(can_data_dir, md_file))
        props['UID'] = generate(size=7)
        bd = BoxExportData(props)
        _export_box_data(bd, box_output_file)
        _export_can_data(table_data, bd.uid, can_output_file)

    print(f'Finished combining all current data from {current_data_dir}')
    return

    
def _read_markdown_data(file_path:str) -> tuple[dict[str,str], pd.DataFrame]:
    """Reading the contents of the markdown file to analyze its structure."""
    with open(file_path, 'r') as file:
        markdown_content = file.read()

    # Split content based on the first occurrence of a table (which starts with a pipe '|')
    parts = re.split(r'(\n\|.*?\n)', markdown_content, maxsplit=1)
    # return parts
    properties_section = parts[0]
    table_section = "".join(parts[1:]) if len(parts) > 1 else ""

    # Extract properties into a dictionary (key-value pairs)
    properties = {}
    for line in properties_section.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            properties[key.strip()] = value.strip()
    

    # Extract table using pandas
    if table_section:
        # Remove leading and trailing pipes and spaces from the table section
        cleaned_table_section = re.sub(r"^\s*\|\s*|\s*\|\s*$", "", table_section, flags=re.MULTILINE)
        table = pd.read_csv(io.StringIO(cleaned_table_section), sep=r"\s*\|\s*", engine='python')
    else:
        table = pd.DataFrame()

    return properties, table

def _export_box_data(box_data:BoxExportData, output_path:str):
    with open(output_path, 'a') as fn:
        data = box_data._csv_export()
        wtr = csv.DictWriter(fn, data[1], lineterminator='\n')        
        wtr.writerow(data[0])

        logger.info(f'Finished exporting box data for {data[0]}')
        return
    
def _export_can_data(can_data:pd.DataFrame, box:str, output_path:str):
    can_data.insert(len(can_data.columns),'Box', [box for _ in range(len(can_data))])
    format_dict = can_data.transpose().to_dict()

    _write_data(output_path, [format_dict[data] for data in format_dict])
        
    logger.info(f'Finishing exporting can data for box: {box}')
    return




#######################################################################################################
###########                                                                                 ###########
###########                                                                                 ###########
###########                                 COMBINED / CLI                                  ###########
###########                                                                                 ###########
###########                                                                                 ###########
#######################################################################################################


def process_all_data(
        prior_data_dir:str, current_data_dir:str, 
        box_output:str, can_output:str
        ):
    process_prior_data(prior_data_dir, can_output, box_output)
    process_current_data(current_data_dir, can_output, box_output)
    print(f'Finished processing all data')
    return


def file_merger(merge_choice:str):
    if merge_choice.strip().capitalize() == 'Y':
        merge_files(output_box, output_can)    
    else:
        print(f'Merge was not accepted.')
    return

def merge_files(box_review_output:str, can_review_output:str):
    if box_review_output == BOX_EXPORT or can_review_output == CAN_EXPORT:
        raise MergerError(box_review_output, can_review_output)
    os.remove(BOX_EXPORT)
    os.remove(CAN_EXPORT)

    os.rename(box_review_output, BOX_EXPORT)
    os.rename(can_review_output, CAN_EXPORT)
    print(f'Successfully merged/overrode can and box output files.')
    return

def overrwrite_output_files(can_file_path:str, box_file_path:str):
    """Overrides final or review data files, depending on whether 2FA is true"""
    with open(can_file_path, 'w') as fn:
            wtr = csv.writer(fn, lineterminator='\n')
            wtr.writerow(HEADERS['Can'])
    with open(box_file_path, 'w') as fn:
        wtr = csv.writer(fn, lineterminator='\n')
        wtr.writerow(HEADERS['Box'])
    return


def str_to_bool(bool_value:str):
    if isinstance(bool_value, bool):
        return value
    if bool_value.lower() in ('true', 'yes', '1'):
        return True
    elif bool_value.lower() in ('false', 'no', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError(f'Boolean str value expected (i.e.: true, false, 0, 1, yes, no). Instead got {bool_value}.')


parser = argparse.ArgumentParser(description='preprocess data')
parser.add_argument('--prior_source_dir', type = str, default=PRIOR_DATA_DIR,
					help='path to folder containing prior data')
parser.add_argument('--current_source_dir', type = str, default=CURRENT_DATA_DIR,
					help='path to folder containing current data')
parser.add_argument('--box_export_filename', type = str, default=BOX_EXPORT,
					help='filename for box export file')
parser.add_argument('--can_export_filename', type = str, default=CAN_EXPORT,
					help='filename for can export file')

parser.add_argument('--two_step', type = str_to_bool, default= True,
                    help='create a separate file to view changes before merging. ')




if __name__ == '__main__':
    args = parser.parse_args()
    
    prior_can_data_dir = os.path.join(args.prior_source_dir, 'can_data_by_box')
    prior_box_data = os.path.join(args.prior_source_dir, 'box_data.csv')

    current_can_data_dir = os.path.join(args.current_source_dir, 'can_data_by_box')
    current_box_data_dir = os.path.join(args.current_source_dir, 'box_data.csv')

    source_data = {
        'prior': {
            'can': prior_can_data_dir,
            'box': prior_box_data,
        },
        'current': {
            'can': current_can_data_dir,
            'box': current_box_data_dir
        }
    }

    print(f'{args.two_step = }')

    if args.two_step is False:
        output_box = os.path.join(SPREADSHEET_DIR, args.box_export_filename)
        output_can = os.path.join(SPREADSHEET_DIR, args.can_export_filename)
    else:
        output_box = os.path.join(SPREADSHEET_DIR, 'box_data_review.csv')
        output_can = os.path.join(SPREADSHEET_DIR, 'can_data_review.csv')
    
    overrwrite_output_files(output_can, output_box)  
    print(
        f'Box Output File: {output_box}',
        f'Can Output File: {output_can}'
    )  

    
    process_all_data(
        prior_data_dir=args.prior_source_dir,
        current_data_dir=args.current_source_dir,
        box_output=output_box,
        can_output=output_can
    )

    if args.two_step is True:
        merge = input('Do you want to override box/can data files with the reviewed files? (Y/N): ')
        file_merger(merge)

    print('Pre-processing complete.')
