from django.urls import path
from etl_app.etl import views as etl_views

urlpatterns = [
    path('api/table-columns/', etl_views.table_columns, name='table_columns'),
] 