from django.db import models
from django.core.validators import RegexValidator
from django.utils import timezone
from typing import Optional, List


PHONE_REGEX = RegexValidator(
    regex=r'^[\d\-\+\s\(\)]{3,30}$',
    message="Phone number may contain digits, spaces, +, -, parentheses."
)


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    class Meta:
        abstract = True



class PdfFile(models.Model):
    pdf_file = models.FileField(verbose_name='PDF file', upload_to='pdf_files',null=True, blank=True)
    def __str__(self):
        return self.pdf_file.name

class Contract(TimeStampedModel):
    file = models.ForeignKey(PdfFile, on_delete=models.CASCADE,blank=True, null=True)
    contract_no = models.CharField(max_length=64, unique=True, db_index=True)
    generated_date = models.DateField(null=True, blank=True)
    raw_text = models.TextField(blank=True)
    embedding = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.contract_no} — {self.generated_date or 'Contract'}"

    class Meta:
        ordering = ['-generated_date', '-created_at']
        
    @classmethod
    def _get_embedder(cls):
        try:
            from sentence_transformers import SentenceTransformer
            return SentenceTransformer('all-MiniLM-L6-v2')
        except Exception:
            return None

    @staticmethod
    def _compute_embedding(text: str) -> Optional[List[float]]:
        if not text:
            return None
        model = Contract._get_embedder()
        if model is None:
            return None
        # sentence-transformers returns numpy array; cast to list of floats
        try:
            vec = model.encode([text], normalize_embeddings=True)
            return vec[0].tolist()  # type: ignore[attr-defined]
        except Exception:
            return None
        
        


class OrganisationDetail(models.Model):
    contract = models.OneToOneField(Contract, on_delete=models.CASCADE, related_name='organization_details')
    type = models.CharField(max_length=128, blank=True)
    ministry = models.CharField(max_length=256, blank=True)
    department = models.CharField(max_length=256, blank=True)
    organisation_name = models.CharField(max_length=256, blank=True)
    office_zone = models.CharField(max_length=256, blank=True)

class BuyerDetail(models.Model):
    contract = models.OneToOneField(Contract, on_delete=models.CASCADE, related_name='buyer')
    designation = models.CharField(max_length=128, blank=True)
    contact_no = models.CharField(max_length=30, validators=[PHONE_REGEX], blank=True)
    email = models.EmailField(blank=True)
    gstin = models.CharField(max_length=32, blank=True)
    address = models.TextField(blank=True)

    def __str__(self):
        return f"Buyer for {self.contract.contract_no}"


class FinancialApproval(models.Model):
    contract = models.OneToOneField(Contract, on_delete=models.CASCADE, related_name='financial_approval')
    ifd_concurrence = models.BooleanField(default=False)
    admin_approval_designation = models.CharField(max_length=256, blank=True)
    financial_approval_designation = models.CharField(max_length=256, blank=True)

    def __str__(self):
        return f"FinancialApproval for {self.contract.contract_no}"


class PayingAuthority(models.Model):
    contract = models.OneToOneField(Contract, on_delete=models.CASCADE, related_name='paying_authority')
    role = models.CharField(max_length=128, blank=True)
    payment_mode = models.CharField(max_length=128, blank=True)
    designation = models.CharField(max_length=128, blank=True)
    email = models.EmailField(blank=True)
    gstin = models.CharField(max_length=32, blank=True)
    address = models.TextField(blank=True)

    def __str__(self):
        return f"PayingAuthority for {self.contract.contract_no}"


class SellerDetail(models.Model):
    contract = models.OneToOneField(Contract, on_delete=models.CASCADE, related_name='seller')
    gem_seller_id = models.CharField(max_length=64, blank=True)
    company_name = models.CharField(max_length=256, blank=True)
    contact_no = models.CharField(max_length=30, validators=[PHONE_REGEX], blank=True)
    email = models.EmailField(blank=True)
    address = models.TextField(blank=True)
    msme_registration_number = models.CharField(max_length=64, blank=True)
    gstin = models.CharField(max_length=32, blank=True)

    def __str__(self):
        return f"{self.company_name or 'Seller'} ({self.contract.contract_no})"


class Product(TimeStampedModel):
    contract = models.ForeignKey(Contract, on_delete=models.CASCADE, related_name='products')
    item_description = models.TextField(blank=True, verbose_name='Item Description')
    product_name = models.CharField(max_length=512)
    brand = models.CharField(max_length=256, blank=True)
    brand_type = models.CharField(max_length=128, blank=True)
    catalogue_status = models.CharField(max_length=256, blank=True)
    selling_as = models.CharField(max_length=256, blank=True)
    category_name_quadrant = models.CharField(max_length=256, blank=True)
    model = models.CharField(max_length=256, blank=True)
    hsn_code = models.CharField(max_length=64, blank=True)

    ordered_quantity = models.CharField(max_length=64, null=True, blank=True)
    unit = models.CharField(max_length=64, blank=True)
    unit_price = models.CharField(max_length=64, null=True, blank=True)
    tax_bifurcation = models.CharField(max_length=64, null=True, blank=True)
    total_price = models.CharField(max_length=64, null=True, blank=True)

    note = models.TextField(blank=True)  # e.g., seller note or undertakings
    embedding = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.product_name} — {self.contract.contract_no}"


class ProductSpecification(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name='specifications')
    category = models.CharField(max_length=128, blank=True)  # e.g. "Dimensions", "Generic", "Additional Information"
    sub_spec = models.CharField(max_length=256, blank=True)  # e.g. "Thickness", "Material"
    value = models.CharField(max_length=512, blank=True)

    def __str__(self):
        return f"{self.product.product_name}: {self.sub_spec} = {self.value}"


class ConsigneeDetail(models.Model):
    # Consignee is often tied to product delivery rows
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name='consignees')
    s_no = models.IntegerField(null=True, blank=True)
    designation = models.CharField(max_length=128, blank=True)
    email = models.EmailField(blank=True)
    contact = models.CharField(max_length=30, validators=[PHONE_REGEX], blank=True)
    gstin = models.CharField(max_length=32, blank=True)
    address = models.TextField(blank=True)
    item = models.CharField(max_length=256, blank=True, verbose_name='Item')
    lot_no = models.CharField(max_length=128, blank=True)
    quantity = models.IntegerField(null=True, blank=True)
    delivery_start = models.DateField(null=True, blank=True)
    delivery_end = models.DateField(null=True, blank=True)
    delivery_to = models.CharField(max_length=256, blank=True)

    def __str__(self):
        return f"Consignee {self.s_no or ''} for {self.product.product_name}"


class EPBGDetail(models.Model):
    contract = models.OneToOneField(Contract, on_delete=models.CASCADE, related_name='epbg')
    detail = models.TextField(blank=True)

    def __str__(self):
        return f"ePBG for {self.contract.contract_no}"


class TermsAndCondition(models.Model):
    contract = models.ForeignKey(Contract, on_delete=models.CASCADE, related_name='terms')
    clause_text = models.TextField()

    def __str__(self):
        short = (self.clause_text[:60] + '...') if len(self.clause_text) > 60 else self.clause_text
        return f"T&C ({short})"
