from . import ALL_BOX_DATA_DF, ALL_CAN_DATA_DF, pd, np, datetime
from .utils import get_multiple_table_properties, get_table_property



def get_average_price() -> float:
    """Calculates average price ($) of all boxes after filtering out NA prices"""
    return round(np.average([float(value) for value in get_table_property('box_data', 'price') if value != 'NA']), 3)

def get_average_drink_velocity() -> float:
    """Calculates average drink velocity (days) of all boxes after filtering out NA DV calculations"""
    return round(np.average([float(value) for value in get_table_property('box_data', 'drink_velocity') if value != 'NA']), 3)
    
def get_average_time_to_start() -> float:
    """Calculates average time to start (days) of all boxes after filtering out NA TTS calculations"""
    return round(np.average([float(value) for value in get_table_property('box_data', 'time_to_start') if value != 'NA']), 3)



    