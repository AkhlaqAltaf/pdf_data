from django.urls import path

from src.apps.bid_record import views

app_name = "bid_record"
urlpatterns = [
    path('upload/', views.upload_bid_document, name="upload_bid_document"),
]
