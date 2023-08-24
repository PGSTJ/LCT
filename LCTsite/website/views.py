from django.shortcuts import render, redirect
from django.http import HttpResponse
from .models import BasicAverages, BoxAverages, BoxTracker, CanData, AbbreviationReferences

# Create your views (endpoints) here.

def tutorial(response):
    return HttpResponse('we out here learning django')

def index(respone):
    return render(respone, 'website/base.html')

def home(respone):
    return render(respone, 'website/base.html')

def add_data(respone):
    title = 'Add Data'
    pg_desc = ''

    return render(respone, 'website/add_data.html', {'page_intro':title, 'page_description':pg_desc, 'title':title, 'verified':True})

def view_graphs(respone):
    return render(respone, 'website/view_graphs.html')

def view_stats(respone):
    return render(respone, 'website/add_data.html')
