from django.db import models
from django.core.exceptions import ValidationError
from functools import lru_cache
from typing import Optional, List

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
    embedding = models.JSONField(null=True, blank=True)
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
        
        try:
            model = cls._get_embedder()
            if model is None:
                print("❌ Could not load sentence transformer model")
                return None
            
            print(f"🔄 Encoding text with {len(text)} characters...")
            vec = model.encode([text], normalize_embeddings=True)
            embedding_list = vec[0].tolist()
            print(f"✅ Embedding computed successfully: {len(embedding_list)} dimensions")
            return embedding_list
            
        except Exception as e:
            print(f"❌ Error computing embedding: {e}")
            return None

    def save(self, *args, **kwargs):
        # Only compute embedding if raw_text is present AND embedding is not already set
        if self.raw_text and not self.embedding:
            print(f"🔄 Computing embedding for bid: {self.bid_number}")
            self.embedding = self._compute_embedding(self.raw_text)
            if self.embedding:
                print(f"✅ Embedding computed: {len(self.embedding)} dimensions")
            else:
                print(f"❌ Failed to compute embedding for bid: {self.bid_number}")

        # Check if bid_number exists in DB (and not the current instance)
        if self.bid_number:
            try:
                existing = BidDocument.objects.get(bid_number=self.bid_number)
                if existing.pk != self.pk:
                    # Update existing record with current data instead of creating new
                    for field in self._meta.fields:
                        if field.name != "id":
                            setattr(existing, field.name, getattr(self, field.name))
                    # Preserve the embedding we computed
                    if self.embedding:
                        existing.embedding = self.embedding
                    existing.save()
                    # Cancel creation of a new record by not calling super().save() here
                    return
            except BidDocument.DoesNotExist:
                # No existing record, proceed with save as new
                pass

        super().save(*args, **kwargs)
