import datetime
import csv

from django.shortcuts import render, redirect
from django.http import HttpResponse
from .models import BasicAverages, BoxAverages, BoxTracker, CanData, AbbreviationReferences

from .data import upload

# Create your views (endpoints) here.

def tutorial(response):
    return HttpResponse('we out here learning django')

def index(respone):
    return render(respone, 'website/base.html')

def home(respone):
    return render(respone, 'website/base.html')

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
    # extract BID in process
    # side effects: location/flavor verify -- NOT NEEDED YET --
    
    if response.FILES.get('bdqu'):
        bd_file = response.FILES['bdqu'].read().decode('utf-8').splitlines()
        format_bd = csv.DictReader(bd_file)

        for box in format_bd:
            all_boxes = [d[0] for d in BoxTracker.objects.values_list('bid')]
            b_id = upload.bid_generator(all_boxes, box['Flavor'])

            bd = BoxTracker(bid=b_id, flavor=box['Flavor'], purchase_date=box['Purchased'], price=box['Price'], location=box['Location'], started=box['Started'], finished=box['Finished'], contributing=False, filled=False)
            bd.save()

    if response.FILES.get('cdqu'):
        cd_file = response.FILES['cdqu'].read().decode('utf-8').splitlines()
        format_cd = csv.DictReader(cd_file)
        box = response.POST['fill_box']
        
        for can in format_cd:
            all_cans = len(CanData.objects.all()) + 1 
            c_id = f'{all_cans}.CD.' + can['Can']

            imass = can['Initial Mass']
            ivol = can['Initial Volume']
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
