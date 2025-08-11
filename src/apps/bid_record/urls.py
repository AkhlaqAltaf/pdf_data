from django.urls import path

from src.apps.bid_record import views

app_name = "bid_record"
urlpatterns = [
    path('upload/', views.upload_bid_document, name="upload_bid_document"),
    path('view/', views.BidTableView.as_view(), name="view_bids"),
    path('api/bid/<int:bid_id>/', views.get_bid_details_api, name="get_bid_details"),
]
