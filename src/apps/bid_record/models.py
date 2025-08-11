from django.db import models
from django.core.exceptions import ValidationError
from functools import lru_cache
from typing import Optional, List

class BidDocument(models.Model):
    file = models.FileField(upload_to='bids/')
    dated = models.DateField(null=True, blank=True)
    source_file = models.CharField(max_length=255, null=True, blank=True)
    bid_number = models.CharField(max_length=100, null=True, blank=True)
    buyer_email = models.EmailField(null=True, blank=True)
    beneficiary = models.CharField(max_length=255, null=True, blank=True)
    delivery_address = models.TextField(null=True, blank=True)
    office_name = models.CharField(max_length=255, null=True, blank=True)
    ministry = models.CharField(max_length=255, null=True, blank=True)
    department = models.CharField(max_length=255, null=True, blank=True)
    organisation = models.CharField(max_length=255, null=True, blank=True)
    estimated_bid_value = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    total_quantity = models.IntegerField(null=True, blank=True)
    contract_period = models.CharField(max_length=255, null=True, blank=True)
    item_category = models.TextField(null=True, blank=True)
    raw_text = models.TextField(null=True, blank=True)
    embedding = models.JSONField(null=True, blank=True)  # Embedding field

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.bid_number} - {self.ministry}"

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_embedder():
        try:
            from sentence_transformers import SentenceTransformer
            return SentenceTransformer('all-MiniLM-L6-v2')
        except Exception:
            return None

    @classmethod
    def _compute_embedding(cls, text: str) -> Optional[List[float]]:
        if not text:
            return None
        model = cls._get_embedder()
        if model is None:
            return None
        try:
            vec = model.encode([text], normalize_embeddings=True)
            return vec[0].tolist()  # type: ignore[attr-defined]
        except Exception:
            return None

    def save(self, *args, **kwargs):
        # Compute embedding if raw_text is present
        if self.raw_text:
            self.embedding = self._compute_embedding(self.raw_text)

        # Check if bid_number exists in DB (and not the current instance)
        if self.bid_number:
            try:
                existing = BidDocument.objects.get(bid_number=self.bid_number)
                if existing.pk != self.pk:
                    # Update existing record with current data instead of creating new
                    for field in self._meta.fields:
                        if field.name != "id":
                            setattr(existing, field.name, getattr(self, field.name))
                    existing.embedding = self.embedding
                    existing.save()
                    # Cancel creation of a new record by not calling super().save() here
                    return
            except BidDocument.DoesNotExist:
                # No existing record, proceed with save as new
                pass

        super().save(*args, **kwargs)
