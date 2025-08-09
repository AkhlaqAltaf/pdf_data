from django.core.management.base import BaseCommand
from django.db import transaction

from src.apps.cont_record.models import Contract, Product

try:
    from sentence_transformers import SentenceTransformer
except Exception:  # pragma: no cover
    SentenceTransformer = None


class Command(BaseCommand):
    help = "Compute and store embeddings for existing Contract and Product records"

    def add_arguments(self, parser):
        parser.add_argument('--batch', type=int, default=128, help='Batch size for embedding computation')

    def handle(self, *args, **options):
        if SentenceTransformer is None:
            self.stderr.write(self.style.ERROR('sentence-transformers not available. Install requirements first.'))
            return

        model = SentenceTransformer('all-MiniLM-L6-v2')
        batch_size = int(options['batch'])

        # Contracts
        contracts = list(Contract.objects.all().select_related('organization_details', 'buyer', 'seller'))
        self.stdout.write(self.style.NOTICE(f'Computing embeddings for {len(contracts)} contracts...'))
        texts = []
        refs = []
        for c in contracts:
            parts = [
                c.contract_no or '',
                c.generated_date.isoformat() if c.generated_date else '',
                getattr(c.organization_details, 'organisation_name', '') if hasattr(c, 'organization_details') else '',
                getattr(c.organization_details, 'department', '') if hasattr(c, 'organization_details') else '',
                getattr(c.seller, 'company_name', '') if hasattr(c, 'seller') else '',
                getattr(c.buyer, 'email', '') if hasattr(c, 'buyer') else '',
                c.raw_text or ''
            ]
            texts.append(' | '.join([p for p in parts if p]))
            refs.append(c)

        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i+batch_size]
            batch_refs = refs[i:i+batch_size]
            if not batch_texts:
                continue
            vecs = model.encode(batch_texts, normalize_embeddings=True)
            with transaction.atomic():
                for ref, vec in zip(batch_refs, vecs):
                    ref.embedding = vec.tolist()
                    ref.save(update_fields=['embedding'])
        self.stdout.write(self.style.SUCCESS('Contract embeddings updated.'))

        # Products
        products = list(Product.objects.select_related('contract').all())
        self.stdout.write(self.style.NOTICE(f'Computing embeddings for {len(products)} products...'))
        p_texts = []
        p_refs = []
        for p in products:
            parts = [p.product_name or '', p.category_name_quadrant or '', p.hsn_code or '', p.note or '']
            combo = ' | '.join([x for x in parts if x])
            if not combo:
                continue
            p_texts.append(combo)
            p_refs.append(p)

        for i in range(0, len(p_texts), batch_size):
            batch_texts = p_texts[i:i+batch_size]
            batch_refs = p_refs[i:i+batch_size]
            if not batch_texts:
                continue
            vecs = model.encode(batch_texts, normalize_embeddings=True)
            with transaction.atomic():
                for ref, vec in zip(batch_refs, vecs):
                    ref.embedding = vec.tolist()
                    ref.save(update_fields=['embedding'])
        self.stdout.write(self.style.SUCCESS('Product embeddings updated.'))


