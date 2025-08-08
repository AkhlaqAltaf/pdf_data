from django.urls import path
from . import views

app_name = 'pdf_record'
urlpatterns = [
    path('', views.upload_pdfs, name='upload_pdfs'),
    path('save-to-database/', views.save_to_database, name='save_to_database'),
]
