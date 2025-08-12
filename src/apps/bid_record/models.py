from django.db import models

class BidDocument(models.Model):
    file = models.FileField(upload_to='bid_documents/', null=True, blank=True)
    dated = models.DateField(null=True, blank=True)
    bid_number = models.CharField(max_length=100, null=True, blank=True)
    beneficiary = models.CharField(max_length=255, null=True, blank=True)
    ministry = models.CharField(max_length=255, null=True, blank=True)
    department = models.CharField(max_length=255, null=True, blank=True)
    organisation = models.CharField(max_length=255, null=True, blank=True)
    contract_period = models.CharField(max_length=255, null=True, blank=True)
    item_category = models.TextField(null=True, blank=True)
    bid_end_datetime = models.CharField(max_length=100, null=True, blank=True)
    bid_open_datetime = models.CharField(max_length=100, null=True, blank=True)
    bid_offer_validity_days = models.IntegerField(null=True, blank=True)
    similar_category = models.TextField(null=True, blank=True)
    mse_exemption = models.CharField(max_length=10, null=True, blank=True)
    source_file = models.CharField(max_length=255, null=True, blank=True)
    
    # Required fields for functionality
    raw_text = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.bid_number} - {self.ministry}"
