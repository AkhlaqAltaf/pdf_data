from django.urls import path
from . import views

app_name = 'pdf_record'
urlpatterns = [
    # path('', views.SemanticSearchView.as_view(), name='search'),
    path('', views.ImportDataView.as_view(), name='import'),
    path('view/', views.ContractTableView.as_view(), name='view'),
    path('save-to-database/', views.SaveInDb.as_view(), name='save_to_database'),
    path('semantic-search/', views.SemanticSearchView.as_view(), name='semantic_search'),
    # path('search/', views.SemanticSearchView.as_view(), name='search'),
]
