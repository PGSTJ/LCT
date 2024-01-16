import datetime
import csv

from django.shortcuts import render, redirect
from django.http import HttpResponse
from .models import BasicAverages, BoxAverages, BoxTracker, CanData, AbbreviationReferences, RawTracker

from .data import upload

def index(respone):
    return render(respone, 'website/base.html')

def home(respone):
    return render(respone, 'website/homepage.html')

def add_data(response):
    all_boxes = [box[0] for box in BoxTracker.objects.filter(filled=False).values_list('bid')]
    

    return render(response, 'website/quick_upload.html', {'available_boxes':all_boxes})
    # return render(respone, 'website/add_data.html', {'page_intro':title, 'page_description':pg_desc, 'title':title, 'verified':True})

def view_graphs(respone):
    return render(respone, 'website/view_graphs.html')

def view_stats(respone):
    return render(respone, 'website/add_data.html')



def insert_data(response):
    # upload Box Data must happen first
    # file headers: Flavor, Purchased, Price, Location, Started, Finished
    # extract BID in process
    # side effects: location/flavor verify -- NOT NEEDED YET --

    if 'submitabbr' in response.POST:
        abbr_names = [response.POST.get(f'abbr-name{i}', '') for i in range(1, 7)]
        abbr_abbreviations = [response.POST.get(f'abbr-abbr{i}', '') for i in range(1, 7)]
        abbr_type = [response.POST.get(f'abbr-type{i}', '') for i in range(1, 7)]
        all_pairs = [(item[0][0],item[0][1], item[1]) for item in zip(zip(abbr_names, abbr_abbreviations), abbr_type)]
        print(all_pairs)
        
        
        for name, abbreviation, type in all_pairs:
            if name != '':
                current_all = len(AbbreviationReferences.objects.all())
                current_type = len(AbbreviationReferences.objects.filter(uid__contains=type))
                id = upload.abbreviation_uid_generator(current_all, current_type, type)
                ar = AbbreviationReferences(uid=id, name=name, abbreviation=abbreviation)
                ar.save()
                print('uploaded')
        
    
    if response.FILES.get('bdqu'):
        bd_file = response.FILES['bdqu'].read().decode('utf-8').splitlines()
        format_bd = csv.DictReader(bd_file)

        for box in format_bd:
            update_raw_tracker(box['Flavor'])
            id = bid_generator(box['Flavor'])
            
            formatting = upload.bd_formatter(id, box['Flavor'])

            for bid, flavor_code in formatting:
                # eventually, start and finish dates will also be flavor specific
                bd = BoxTracker(bid=bid, flavor=flavor_code, purchase_date=box['Purchased'], price=box['Price'], location=box['Location'], started=box['Started'], finished=box['Finished'], contributing=False, filled=False)
                bd.save()

    if response.FILES.get('cdqu'):
        cd_file = response.FILES['cdqu'].read().decode('utf-8').splitlines()
        format_cd = csv.DictReader(cd_file)
        box = response.POST['fill_box']
        
        for can in format_cd:
            all_cans = len(CanData.objects.all()) + 1 
            c_id = f'{all_cans}.CD.' + can['Can']

            imass = can['Initial Mass']
            ivol = can['Initial Volumne']
            fmass = can['Final Mass']
            fvol = can['Final Volume']

            plm = upload.percent_loss_calculator(imass, fmass)
            plv = upload.percent_loss_calculator(ivol, fvol)

            cd = CanData(cid=c_id, bid=BoxTracker.objects.get(bid=box), initial_grams=imass, initial_floz=ivol, final_grams=fmass, final_floz=fvol, finished=can['Finished'], percent_remaining_g=plm, percent_remaining_floz=plv)
            cd.save()

            bx = BoxTracker.objects.get(bid=box)
            bx.filled = True
            bx.save()
    
    # return HttpResponse('Uploaded Box Data')
    
    return redirect('/add_data')

def update_raw_tracker(flavor:str) -> bool:
    total = RawTracker.objects.get(value='TotalBoxes')
    total.amount += 1
    total.save()
    
    all_values = [data[0] for data in RawTracker.objects.values_list('value')]
    if flavor not in all_values:
        update = RawTracker(value=flavor, amount=1)
    elif flavor in all_values:
        update = RawTracker.objects.get(value=flavor)
        update.amount += 1
    update.save()

    return True
 
def bid_generator(flavor_code:str) -> str:
    overall = RawTracker.objects.get(value='TotalBoxes').amount
    flavor_amt = RawTracker.objects.get(value=flavor_code).amount
    return f'{overall}.{flavor_code}.{flavor_amt}'



def test(response):
    d = bid_generator('PSF')


    return HttpResponse(d)
