from django.urls import path
from . import views

# urls of the specific views of this application

urlpatterns = [
    path('', views.index, name='index'),
    path('home', views.home, name='home'),
    path('add_data', views.add_data, name='add_data'),
    path('view_graphs', views.view_graphs, name='view_graphs'),
    path('view_stats', views.view_stats, name='view_stats'),
    path('insert_data', views.insert_data, name='insert_data'),
    path('test', views.test, name='test'),
]