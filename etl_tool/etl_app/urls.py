from django.urls import path, include
from . import views
from .views import save_pipeline, update_pipeline, delete_pipeline

urlpatterns = [
    path('', views.home, name='home'),
    path('create/', views.create_job, name='create_job'),
    path('api/jobs/', views.job_list, name='job_list'),
    path('api/table_data/', views.table_data, name='table_data'),
    path('api/jobs/<int:job_id>/delete/', views.delete_job, name='delete_job'),
    path('api/node_status/', views.node_status, name='node_status'),
    path('api/save_pipeline/', save_pipeline, name='save_pipeline'),
    path('api/update_pipeline/', update_pipeline, name='update_pipeline'),
    path('api/delete_pipeline/', delete_pipeline, name='delete_pipeline'),
    path('', include('etl_app.etl_tool.urls')),
]
