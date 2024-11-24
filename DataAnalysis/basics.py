from . import ALL_BOX_DATA_DF, ALL_CAN_DATA_DF, pd, np, datetime



def get_average_price():
    return round(np.average([float(value) for value in ALL_BOX_DATA_DF['price'].to_numpy() if value != 'NA']), 2)

def get_average_drink_velocity():
    



    date_data = ALL_BOX_DATA_DF.get(['started', 'finished'])
    dct_version:dict[str, dict[int,str]] = date_data.to_dict()
    # pairs start and finish dates for direct comparison
    paired = [(start,finish) for start,finish in zip(dct_version['started'].values(), dct_version['finished'].values()) if start != 'NA' or finish != 'NA']
    calculated_differences = [_get_time_difference(pairs) for pairs in paired if _get_time_difference(pairs) != None]
    
    return np.average(calculated_differences)


def _get_time_difference(date_data:tuple[str,str], date_format:str='%m/%d/%Y') -> int | None:
    """calculates date differences between two dates and returns none if lacking date data"""
    if 'NA' in date_data:
        return
    datetime_version = [datetime.datetime.strptime(date, date_format) for date in date_data]
    difference = datetime_version[1] - datetime_version[0]
    return difference.days
    
    
def calculate_drink_velocity(start_date:datetime.date, end_date:datetime.date):
    d = end_date - start_date
    