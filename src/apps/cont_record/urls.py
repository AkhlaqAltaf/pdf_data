from django.urls import path
from .views import PDFUploadView

app_name = 'pdf_record'
urlpatterns = [
    path("", PDFUploadView.as_view(), name="upload_pdfs"),
]
