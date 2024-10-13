from . import SPREADSHEET_DIR, pd, re, io, os, csv, logger
import traceback

BOX_EXPORT = 'exported_box_data.csv'
CAN_EXPORT = 'exported_can_data.csv'


class BoxUploadData():
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
    


def extract_export_data() -> tuple[list[BoxUploadData], list[pd.DataFrame]]:
    """Extracts box and can data from Notion CSV and MD files, then exports into master CSVs"""
    md_can_data_dir = SPREADSHEET_DIR / 'LCT_2024/can_data_by_box'
    for file in os.listdir(md_can_data_dir):
        props, table_data = read_markdown_data(md_can_data_dir / file)
        props['UID'] = _format_box_uid(file.split())
        # if box == '12BP':
        bd = BoxUploadData(props)
        export_box_data(bd)
        export_can_data(table_data,bd.uid)

    print('Done with extracting and writing')
        # format for csv export to update master csvs


def _format_box_uid(file_name:list[str]):
    """Accounts for costco packs which have an extra subdivision detailing the flavors within"""
    if len(file_name[1]) > 2:
        return file_name[0]
    return file_name[0]+file_name[1]

def read_markdown_data(file_path:str):
    """Reading the contents of the markdown file to analyze its structure."""
    with open(file_path, 'r') as file:
        markdown_content = file.read()

    return extract_properties_and_table(markdown_content)

def extract_properties_and_table(content:str) -> tuple[dict[str,str], pd.DataFrame]:
    # Split content based on the first occurrence of a table (which starts with a pipe '|')
    parts = re.split(r'(\n\|.*?\n)', content, maxsplit=1)
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


def export_box_data(box_data:BoxUploadData):
    with open(BOX_EXPORT, 'a') as fn:
        data = box_data._csv_export()
        wtr = csv.DictWriter(fn, data[1], lineterminator='\n')        
        wtr.writerow(data[0])

        logger.info(f'Finished exporting box data for {data[0]}')
        return

def export_can_data(can_data:pd.DataFrame, box:str):
    hdr = [col_name for col_name in can_data.columns]+['Box']
    can_data.insert(len(can_data.columns),'Box', [box for _ in range(len(can_data))])
    format_dict = can_data.transpose().to_dict()
    formatted_data:list[dict[str,str]] = [format_dict[data] for data in format_dict][1:]

    with open(CAN_EXPORT, 'a') as fn:
        wtr = csv.DictWriter(fn, hdr, lineterminator='\n')
        
        wtr.writerows(formatted_data)
        
    logger.info(f'Finishing exporting can data for box: {box}')
    return
            



