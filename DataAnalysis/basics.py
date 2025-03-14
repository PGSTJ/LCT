from . import pd, np, datetime
from .database import utils as dbu

class InvalidAbbreviation(Exception):
    def __init__(self, message:str=None, **kwargs):
        abbrv = kwargs['abbreviation'] if kwargs['abbreviation'] else None
        self.message = message if message else f'The abbreviation: {abbrv}, does not exist in the reference data table.'
        super().__init__(self.message)




# working from purely raw data (no prior analyses/calculations)
def get_average_empty_can_stats():
    """Calculate average weight, """
    for boxes in dbu.get_table_property('box_data', 'box_id'):
        box = dbu.Box(boxes)
        box._retrieve_data()
        return box





# working from semi processed data (one level of analysis/calculation)
def get_average_price(flavor_abbreviation:str=None) -> float:
    """Calculate overall average, or specify a flavor to calculate average for that specific flavor"""

    if flavor_abbreviation == None:
        return round(np.average([float(value) for value in dbu.get_table_property('box_data', 'price') if value != 'NA']), 2)
    if dbu.check_abbreviation_existence(flavor_abbreviation):
        return round(np.average([float(value) for value in dbu.get_table_property('box_data', 'price', 'flavor', flavor_abbreviation) if value != 'NA']), 2)
    else:
        raise InvalidAbbreviation(abbreviation=flavor_abbreviation)


def get_average_drink_velocity(flavor_abbreviation:str=None) -> float:
    """Calculate overall DV, or specify a flavor to calculate DV for that specific flavor"""
    
    if flavor_abbreviation == None:
        return round(np.average([float(value) for value in dbu.get_table_property('box_data', 'drink_velocity') if value != 'NA']), 2)
    if dbu.check_abbreviation_existence(flavor_abbreviation):
        return round(np.average([float(value) for value in dbu.get_table_property('box_data', 'drink_velocity', 'flavor', flavor_abbreviation) if value != 'NA']), 2)
    else:
        raise InvalidAbbreviation(abbreviation=flavor_abbreviation)



def _get_time_difference(date_data:tuple[str,str], date_format:str='%m/%d/%Y') -> int | None:
    """calculates date differences between two dates and returns none if lacking date data"""
    if 'NA' in date_data:
        return
    datetime_version = [datetime.datetime.strptime(date, date_format) for date in date_data]
    difference = datetime_version[1] - datetime_version[0]
    return difference.days
    
