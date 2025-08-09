from django.urls import path
from . import views

app_name = 'pdf_record'
urlpatterns = [
    path('', views.upload_pdfs, name='upload_pdfs'),
    path('save-to-database/', views.save_to_database, name='save_to_database'),
    path('export-excel/', views.export_to_excel, name='export_to_excel'),
    path('data-details/', views.data_details, name='data_details'),
    path('all-data-table/', views.all_data_table, name='all_data_table'),
    path('export-all-data-excel/', views.export_all_data_excel, name='export_all_data_excel'),
]
