import json


COMBO_PACKS = r'app\data\combos.json'
FLVR_AMT_VERIFICATION = 7 # TODO: Must increase value by 1 once >999 entries

# all functions pertaining to data handling during model/DB uploads

def bid_generator(current_boxes:list, flavor:str) -> str:
    """ Creates unique box ID during box data uploads """
    
    # total number of boxes in database by taking all numbers preceding the first period and adding one
    if len(current_boxes) == 0:
        overall_number:int = 1
    else:
        overall_number:int = int(current_boxes[-1][:current_boxes[-1].index('.')]) + 1

    # print(f'ov #: {overall_number}')

    # extract all IDs with this flavor for relative flavor amount
    flvr_amt:int = len([id for id in current_boxes if flavor in id[:FLVR_AMT_VERIFICATION]]) + 1

    return f'{overall_number}.{flavor}.{flvr_amt}'

def percent_loss_calculator(initial, final):
    """ Calculates the amount of undrunken la croix, or the percentage of the weight held by the can only """
    if initial == '' or final == '':
        return 0
    else:
        i = float(initial)
        f = float(final)
        return round((f / i)*100, 3)
    
def bd_formatter(bid, flavor_code:str) -> list:
    """ Handles Flavor and UID processing, returns list of tuples (BID, flavor) to upload """
    pack_list = _pack_abbreviations()

    if flavor_code in pack_list:
        # new_bid = update_bid(bid) 
        bid += "."
        flavor_list = pack_list[flavor_code].strip('][').split(', ')
        # updates BID with individual flavor abbreviations
        return [(bid+flavor, flavor) for flavor in flavor_list]
    else:
        return [(bid, flavor_code)]
            

def update_bid(id:str):
    """ Fixes flavor amount tracking to not include flavors within combo packs and formats for combo specific ID"""
    pass


    
def _pack_abbreviations():
    """ Grabs associated flavors for combo packs, identified by the Pack Abbreviation """
    with open(COMBO_PACKS, 'r') as fn:
        return json.load(fn)
    
def abbreviation_uid_generator(total_length, type_length, type) -> str:
    new_type = int(type_length) + 1
    new_total = int(total_length) + 1
    
    return f'{new_total}.{type}.{new_type}'


if __name__ == '__main__':
    e = bd_formatter([], 'BBG')
    print(e)


    