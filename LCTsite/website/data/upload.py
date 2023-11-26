# all functions pertaining to data handling during model/DB uploads

def bid_generator(current_boxes:list, flavor:str):
    
    # total number of boxes in database
    overall_number:int = len(current_boxes) + 1

    # extract all IDs with this flavor for relative flavor amount
    flvr_amt:int = len([id for id in current_boxes if flavor in id]) + 1

    return f'{overall_number}.{flavor}.{flvr_amt}'

def percent_loss_calculator(initial, final):
    if initial == '' or final == '':
        return 0
    else:
        i = float(initial)
        f = float(final)
        return round((f / i)*100, 3)
    
def valid_value(value) -> float | int:
    if value == '':
        return -1
    else:
        return float(value)

if __name__ == '__main__':
    e = bid_generator(['1.GSP.1'], 'PF')
    print(e)


    