"""
Contains Objects used to represent Boxes and Cans as Database Tables


"""


from .config import (generate, DATE_FORMAT, datetime, logger)


class BoxCanBase():
    def __init__(self, data:dict[str,str]):
        self.id = generate(size=7)
        self.output_file:str = ''

    def _fill_data(self, data):
        for info in data:            
            if info in self.__dict__:
                self.__dict__[info] = data[info]
        return
    
    def _format_dates(self, date_format:str=DATE_FORMAT):
        """ Ensures dates are the same format - MM/DD/YYYY """
        def check_date_format(date:str) -> datetime.datetime:
            if '/' in date:
                return datetime.datetime.strptime(date, DATE_FORMAT)
            elif ',' in date: 
                return datetime.datetime.strptime(date, '%B %d, %Y')
            elif date == 'NA' or date == '':
                return None
            else:
                raise ValueError(f'Value might not be a date: {date}')


        class_dates = {prop:check_date_format(self.__dict__[prop]) for prop in self.__dict__ if 'date' in prop}
        formatted_dates = {prop:class_dates[prop].strftime(date_format) for prop in class_dates if class_dates[prop] is not None}
        self._fill_data(formatted_dates)
        logger.info(f'Updated the date format parameters at {[i for i in formatted_dates]} for {self.id}')
        return

    def display(self):
        """ Prints key:value description of class attributes """
        for param in self.__dict__:
            print(f'{param}: {self.__dict__[param]}')
        print()

    def write_export_format(self) -> dict[str,str]:
        """ Formats data for file export """
        if isinstance(self, BoxAllData):
            pk = 'box_id'
        elif isinstance(self, BoxFlavorData):
            pk = 'bfid'
        elif isinstance(self, CanData):
            pk = 'can_id'

        data = {pk: self.__dict__['id']}
        values = {info:self.__dict__[info] for info in self.__dict__ if info not in ['output_file', 'id']}
        data.update(values)
        
        return data

            

class BoxFlavorData(BoxCanBase):
    def __init__(self, data:dict[str,str], generated_bid:str):
        super().__init__(data)
        self.box_id = generated_bid
        self.flavor:str = ''
        self.start_date:str = ''
        self.finish_date:str = ''

        self._fill_data(data)
        self._format_dates()

class BoxAllData(BoxCanBase):
    def __init__(self, data:dict[str,str]):
        super().__init__(data)
        self.purchase_date:str = ''
        self.price:float = 0.00
        self.location:str = ''
        self.og_id:str = ''

        self._fill_data(data)
        self._format_dates()
        
                
class CanData(BoxCanBase):
    def __init__(self, data:dict[str,str]):
        super().__init__(data)
        self.id = data['can_id']
        self.box_id:str = ''
        self.initial_mass:int = 0
        self.initial_volume:float = 0.0
        self.final_mass:int = 0
        self.final_volume = 0.0
        self.finish_status:str = ''
        
        self._fill_data(data) # need to fill data before PR calculations

        self.percent_mass_remaining:float | None = self.calculate_percentage_remaining(self.initial_mass, self.final_mass)
        self.percent_volume_remaining:float | None = self.calculate_percentage_remaining(self.initial_volume, self.final_volume)


    # TODO Will likely take this out and calculate during a pre-processing calculation stage
    @staticmethod
    def calculate_percentage_remaining(inital:str, final:str):
        if inital == '' or final == '':
            return None
        try:
            pr = (float(final) / float(inital)) * 100
            return round(pr, 2)
        except ZeroDivisionError:
            return None

