from django.urls import path
from . import views

app_name = 'pdf_record'
urlpatterns = [
    path('', views.ImportDataView.as_view(), name='home'),
    path('import/', views.ImportDataView.as_view(), name='import'),

    path('view/', views.ContractTableView.as_view(), name='view'),
    path('save-to-database/', views.SaveInDb.as_view(), name='save_to_database'),
    path('search/', views.SemanticSearchView.as_view(), name='search'),
]
