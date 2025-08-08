from django.urls import path
from .views import upload_pdfs

app_name = 'pdf_record'
urlpatterns = [
    path("", upload_pdfs, name="upload_pdfs"),
]
