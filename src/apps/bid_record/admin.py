from django.contrib import admin
from .models import BidDocument

@admin.register(BidDocument)
class BidDocumentAdmin(admin.ModelAdmin):
    list_display = ("bid_number", "dated", "ministry", "department", "organisation")
