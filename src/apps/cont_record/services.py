from typing import Dict, Any, List, Optional
from functools import lru_cache
from django.db import transaction
from datetime import datetime
import re

from .models import (
    Contract, BuyerDetail, FinancialApproval, PayingAuthority,
    SellerDetail, Product, ProductSpecification, ConsigneeDetail,
    EPBGDetail, TermsAndCondition, OrganisationDetail
)


class ContractDataService:
    """Service for saving extracted contract data to Django models."""

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
        # sentence-transformers returns numpy array; cast to list of floats
        try:
            vec = model.encode([text], normalize_embeddings=True)
            return vec[0].tolist()  # type: ignore[attr-defined]
        except Exception:
            return None
    
    @staticmethod
    def _parse_date(date_str: str) -> Optional[datetime]:
        """Parse various date formats."""
        if not date_str:
            return None
        
        date_str = date_str.strip()
        patterns = [
            "%d-%b-%Y", "%d-%B-%Y", "%d %b %Y", "%d %B %Y",
            "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%y", "%d %b %y"
        ]
        
        for pattern in patterns:
            try:
                return datetime.strptime(date_str, pattern)
            except ValueError:
                continue
        
        return None
    
    @staticmethod
    def _parse_number(value: str) -> Optional[float]:
        """Parse numeric values from strings."""
        if not value:
            return None
        
        # Remove non-numeric characters except decimal and comma
        cleaned = re.sub(r'[^\d\.,\-]', '', str(value))
        cleaned = cleaned.replace(',', '')
        
        try:
            return float(cleaned)
        except ValueError:
            return None
    
    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        return text.strip()
    
    @classmethod
    def save_contract_data(cls, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Save extracted contract data to Django models.
        
        Returns:
            Dict with success status and created/updated objects
        """
        result = {
            'success': False,
            'contract': None,
            'errors': [],
            'warnings': []
        }
        
        try:
            with transaction.atomic():
                # Extract contract data
                contract_data = extracted_data.get('contract', {})
                contract_no = contract_data.get('contract_no')
                
                if not contract_no:
                    result['errors'].append("Contract number is required")
                    return result
                
                # Check if contract already exists
                contract, created = Contract.objects.get_or_create(
                    contract_no=contract_no,
                    defaults={
                        'generated_date': cls._parse_date(contract_data.get('generated_date')),
                        'raw_text': extracted_data.get('raw_text_preview', ''),
                        'embedding': cls._compute_embedding(
                            ' '.join(filter(None, [
                                contract_no,
                                extracted_data.get('raw_text_preview', '')
                            ]))
                        )
                    }
                )
                
                if not created:
                    result['warnings'].append(f"Contract {contract_no} already exists")
                
                result['contract'] = contract
                
                # Save organization details
                cls._save_organisation_details(contract, extracted_data.get('organisation', {}))
                
                # Save buyer details
                cls._save_buyer_details(contract, extracted_data.get('buyer', {}))
                
                # Save financial approval
                cls._save_financial_approval(contract, extracted_data.get('financial_approval', {}))
                
                # Save paying authority
                cls._save_paying_authority(contract, extracted_data.get('paying_authority', {}))
                
                # Save seller details
                cls._save_seller_details(contract, extracted_data.get('seller', {}))
                
                # Save products
                cls._save_products(contract, extracted_data.get('products', []))
                
                # Save consignees
                cls._save_consignees(contract, extracted_data.get('consignees', []))
                
                # Save specifications
                cls._save_specifications(contract, extracted_data.get('specifications', []))
                
                # Save terms and conditions
                cls._save_terms(contract, extracted_data.get('terms', []))
                
                # Save EPBG details
                cls._save_epbg_details(contract, extracted_data.get('epbg', ''))
                
                result['success'] = True
                
        except Exception as e:
            result['errors'].append(f"Database error: {str(e)}")
        
        return result
    
    @classmethod
    def _save_organisation_details(cls, contract: Contract, org_data: Dict[str, Any]):
        """Save organization details."""
        if not org_data:
            return
        
        org_detail, created = OrganisationDetail.objects.get_or_create(
            contract=contract,
            defaults={
                'type': cls._clean_text(org_data.get('type', '')),
                'ministry': cls._clean_text(org_data.get('ministry', '')),
                'department': cls._clean_text(org_data.get('department', '')),
                'organisation_name': cls._clean_text(org_data.get('organisation_name', '')),
                'office_zone': cls._clean_text(org_data.get('office_zone', ''))
            }
        )
        
        if not created:
            # Update existing record
            org_detail.type = cls._clean_text(org_data.get('type', org_detail.type))
            org_detail.ministry = cls._clean_text(org_data.get('ministry', org_detail.ministry))
            org_detail.department = cls._clean_text(org_data.get('department', org_detail.department))
            org_detail.organisation_name = cls._clean_text(org_data.get('organisation_name', org_detail.organisation_name))
            org_detail.office_zone = cls._clean_text(org_data.get('office_zone', org_detail.office_zone))
            org_detail.save()
    
    @classmethod
    def _save_buyer_details(cls, contract: Contract, buyer_data: Dict[str, Any]):
        """Save buyer details."""
        if not buyer_data:
            return
        
        buyer_detail, created = BuyerDetail.objects.get_or_create(
            contract=contract,
            defaults={
                'designation': cls._clean_text(buyer_data.get('designation', '')),
                'contact_no': cls._clean_text(buyer_data.get('contact_no', '')),
                'email': cls._clean_text(buyer_data.get('email', '')),
                'gstin': cls._clean_text(buyer_data.get('gstin', '')),
                'address': cls._clean_text(buyer_data.get('address', ''))
            }
        )
        
        if not created:
            buyer_detail.designation = cls._clean_text(buyer_data.get('designation', buyer_detail.designation))
            buyer_detail.contact_no = cls._clean_text(buyer_data.get('contact_no', buyer_detail.contact_no))
            buyer_detail.email = cls._clean_text(buyer_data.get('email', buyer_detail.email))
            buyer_detail.gstin = cls._clean_text(buyer_data.get('gstin', buyer_detail.gstin))
            buyer_detail.address = cls._clean_text(buyer_data.get('address', buyer_detail.address))
            buyer_detail.save()
    
    @classmethod
    def _save_financial_approval(cls, contract: Contract, financial_data: Dict[str, Any]):
        """Save financial approval details."""
        if not financial_data:
            return
        
        financial_approval, created = FinancialApproval.objects.get_or_create(
            contract=contract,
            defaults={
                'ifd_concurrence': bool(financial_data.get('ifd_concurrence')),
                'admin_approval_designation': cls._clean_text(financial_data.get('admin_approval_designation', '')),
                'financial_approval_designation': cls._clean_text(financial_data.get('financial_approval_designation', ''))
            }
        )
        
        if not created:
            financial_approval.ifd_concurrence = bool(financial_data.get('ifd_concurrence', financial_approval.ifd_concurrence))
            financial_approval.admin_approval_designation = cls._clean_text(financial_data.get('admin_approval_designation', financial_approval.admin_approval_designation))
            financial_approval.financial_approval_designation = cls._clean_text(financial_data.get('financial_approval_designation', financial_approval.financial_approval_designation))
            financial_approval.save()
    
    @classmethod
    def _save_paying_authority(cls, contract: Contract, authority_data: Dict[str, Any]):
        """Save paying authority details."""
        if not authority_data:
            return
        
        paying_authority, created = PayingAuthority.objects.get_or_create(
            contract=contract,
            defaults={
                'role': cls._clean_text(authority_data.get('role', '')),
                'payment_mode': cls._clean_text(authority_data.get('payment_mode', '')),
                'designation': cls._clean_text(authority_data.get('designation', '')),
                'email': cls._clean_text(authority_data.get('email', '')),
                'gstin': cls._clean_text(authority_data.get('gstin', '')),
                'address': cls._clean_text(authority_data.get('address', ''))
            }
        )
        
        if not created:
            paying_authority.role = cls._clean_text(authority_data.get('role', paying_authority.role))
            paying_authority.payment_mode = cls._clean_text(authority_data.get('payment_mode', paying_authority.payment_mode))
            paying_authority.designation = cls._clean_text(authority_data.get('designation', paying_authority.designation))
            paying_authority.email = cls._clean_text(authority_data.get('email', paying_authority.email))
            paying_authority.gstin = cls._clean_text(authority_data.get('gstin', paying_authority.gstin))
            paying_authority.address = cls._clean_text(authority_data.get('address', paying_authority.address))
            paying_authority.save()
    
    @classmethod
    def _save_seller_details(cls, contract: Contract, seller_data: Dict[str, Any]):
        """Save seller details."""
        if not seller_data:
            return
        
        seller_detail, created = SellerDetail.objects.get_or_create(
            contract=contract,
            defaults={
                'gem_seller_id': cls._clean_text(seller_data.get('gem_seller_id', '')),
                'company_name': cls._clean_text(seller_data.get('company_name', '')),
                'contact_no': cls._clean_text(seller_data.get('contact_no', '')),
                'email': cls._clean_text(seller_data.get('email', '')),
                'address': cls._clean_text(seller_data.get('address', '')),
                'msme_registration_number': cls._clean_text(seller_data.get('msme_registration_number', '')),
                'gstin': cls._clean_text(seller_data.get('gstin', ''))
            }
        )
        
        if not created:
            seller_detail.gem_seller_id = cls._clean_text(seller_data.get('gem_seller_id', seller_detail.gem_seller_id))
            seller_detail.company_name = cls._clean_text(seller_data.get('company_name', seller_detail.company_name))
            seller_detail.contact_no = cls._clean_text(seller_data.get('contact_no', seller_detail.contact_no))
            seller_detail.email = cls._clean_text(seller_data.get('email', seller_detail.email))
            seller_detail.address = cls._clean_text(seller_data.get('address', seller_detail.address))
            seller_detail.msme_registration_number = cls._clean_text(seller_data.get('msme_registration_number', seller_detail.msme_registration_number))
            seller_detail.gstin = cls._clean_text(seller_data.get('gstin', seller_detail.gstin))
            seller_detail.save()
    
    @classmethod
    def _save_products(cls, contract: Contract, products_data: List[Dict[str, Any]]):
        """Save products."""
        for product_data in products_data:
            if not product_data.get('product_name'):
                continue
            
            product, created = Product.objects.get_or_create(
                contract=contract,
                product_name=cls._clean_text(product_data.get('product_name')),
                defaults={
                    'brand': cls._clean_text(product_data.get('brand', '')),
                    'brand_type': cls._clean_text(product_data.get('brand_type', '')),
                    'catalogue_status': cls._clean_text(product_data.get('catalogue_status', '')),
                    'selling_as': cls._clean_text(product_data.get('selling_as', '')),
                    'category_name_quadrant': cls._clean_text(product_data.get('category_name_quadrant', '')),
                    'model': cls._clean_text(product_data.get('model', '')),
                    'hsn_code': cls._clean_text(product_data.get('hsn_code', '')),
                    'ordered_quantity': cls._parse_number(product_data.get('ordered_quantity')),
                    'unit': cls._clean_text(product_data.get('unit', '')),
                    'unit_price': cls._parse_number(product_data.get('unit_price')),
                    'tax_bifurcation': cls._parse_number(product_data.get('tax_bifurcation')),
                    'total_price': cls._parse_number(product_data.get('total_price')),
                    'note': cls._clean_text(product_data.get('note', ''))
                }
            )
            
            if not created:
                # Update existing product
                product.brand = cls._clean_text(product_data.get('brand', product.brand))
                product.brand_type = cls._clean_text(product_data.get('brand_type', product.brand_type))
                product.catalogue_status = cls._clean_text(product_data.get('catalogue_status', product.catalogue_status))
                product.selling_as = cls._clean_text(product_data.get('selling_as', product.selling_as))
                product.category_name_quadrant = cls._clean_text(product_data.get('category_name_quadrant', product.category_name_quadrant))
                product.model = cls._clean_text(product_data.get('model', product.model))
                product.hsn_code = cls._clean_text(product_data.get('hsn_code', product.hsn_code))
                product.ordered_quantity = cls._parse_number(product_data.get('ordered_quantity')) or product.ordered_quantity
                product.unit = cls._clean_text(product_data.get('unit', product.unit))
                product.unit_price = cls._parse_number(product_data.get('unit_price')) or product.unit_price
                product.tax_bifurcation = cls._parse_number(product_data.get('tax_bifurcation')) or product.tax_bifurcation
                product.total_price = cls._parse_number(product_data.get('total_price')) or product.total_price
                product.note = cls._clean_text(product_data.get('note', product.note))
                product.save()
    
    @classmethod
    def _save_consignees(cls, contract: Contract, consignees_data: List[Dict[str, Any]]):
        """Save consignee details."""
        for consignee_data in consignees_data:
            if not consignee_data.get('designation') and not consignee_data.get('address'):
                continue
            
            # Find associated product if lot_no is provided
            product = None
            if consignee_data.get('lot_no'):
                product = Product.objects.filter(contract=contract).first()
            
            consignee = ConsigneeDetail.objects.create(
                product=product,
                s_no=consignee_data.get('s_no'),
                designation=cls._clean_text(consignee_data.get('designation', '')),
                email=cls._clean_text(consignee_data.get('email', '')),
                contact=cls._clean_text(consignee_data.get('contact', '')),
                gstin=cls._clean_text(consignee_data.get('gstin', '')),
                address=cls._clean_text(consignee_data.get('address', '')),
                lot_no=cls._clean_text(consignee_data.get('lot_no', '')),
                quantity=cls._parse_number(consignee_data.get('quantity')),
                delivery_start=cls._parse_date(consignee_data.get('delivery_start')),
                delivery_end=cls._parse_date(consignee_data.get('delivery_end')),
                delivery_to=cls._clean_text(consignee_data.get('delivery_to', ''))
            )
    
    @classmethod
    def _save_specifications(cls, contract: Contract, specifications_data: List[Dict[str, Any]]):
        """Save product specifications."""
        for spec_data in specifications_data:
            if not spec_data.get('sub_spec') or not spec_data.get('value'):
                continue
            
            # Find associated product
            product = Product.objects.filter(contract=contract).first()
            if not product:
                continue
            
            ProductSpecification.objects.create(
                product=product,
                category=cls._clean_text(spec_data.get('category', '')),
                sub_spec=cls._clean_text(spec_data.get('sub_spec', '')),
                value=cls._clean_text(spec_data.get('value', ''))
            )
    
    @classmethod
    def _save_terms(cls, contract: Contract, terms_data: List[str]):
        """Save terms and conditions."""
        for term_text in terms_data:
            if term_text.strip():
                TermsAndCondition.objects.create(
                    contract=contract,
                    clause_text=cls._clean_text(term_text)
                )
    
    @classmethod
    def _save_epbg_details(cls, contract: Contract, epbg_text: str):
        """Save EPBG details."""
        if not epbg_text.strip():
            return
        
        EPBGDetail.objects.get_or_create(
            contract=contract,
            defaults={'detail': cls._clean_text(epbg_text)}
        )
