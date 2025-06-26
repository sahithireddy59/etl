from django.urls import path, include
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('create/', views.create_job, name='create_job'),
    path('api/jobs/', views.job_list, name='job_list'),
    path('api/table_data/', views.table_data, name='table_data'),
    path('api/jobs/<int:job_id>/delete/', views.delete_job, name='delete_job'),
    path('', include('etl_app.etl_tool.urls')),
]
