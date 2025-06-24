from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('create/', views.create_job, name='create_job'),
    path('api/jobs/', views.job_list, name='job_list'),
    path('api/table_data/', views.table_data, name='table_data'),
]
