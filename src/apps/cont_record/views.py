import json
import os
import re
import zipfile
from datetime import datetime
from decimal import Decimal
from io import BytesIO
from urllib.parse import urlparse
import pandas as pd
from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.db import transaction, connection
from django.db.models import Q, Prefetch
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
from django.views import View
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie
from django.utils.decorators import method_decorator
from django.views.decorators.http import require_http_methods

from src.utils.contract_parsers import parse_contract_text_to_json
from src.utils.extract_text import read_pdf_with_structure, _extract_english_from_pdf
from .models import (
    Contract, OrganisationDetail, BuyerDetail, FinancialApproval,
    PayingAuthority, SellerDetail, Product, ProductSpecification,
    ConsigneeDetail, EPBGDetail, TermsAndCondition, PdfFile
)
from ...utils.save_data_helper import safe_str
from ...utils.save_helper import extract_from_tables, parse_int, parse_decimal, extract_email, \
    extract_phone, parse_date
from ...utils.table_helper import safe_decimal_from_raw, safe_int_from_raw

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
except Exception:
    SentenceTransformer = None
    np = None

_EMBEDDER = None

def get_embedder():
    global _EMBEDDER
    if SentenceTransformer is None:
        return None
    if _EMBEDDER is None:
        try:
            _EMBEDDER = SentenceTransformer('all-MiniLM-L6-v2')
        except Exception:
            _EMBEDDER = None
    return _EMBEDDER


class ImportDataView(View):
    template_name = "contracts/import_data.html"

    def get(self, request):
        return render(request, self.template_name)

    def post(self, request):
        uploaded_files = request.FILES.getlist("data_file")

        print("UPLOADED FILE...")
        fs = FileSystemStorage()
        all_results = []

        if not uploaded_files:
            return render(request, self.template_name, {"error": "Please upload at least one PDF or ZIP file."})

        for uploaded_file in uploaded_files:
            temp_files = []
            saved_file = None

            try:
                # Handle ZIP file: extract only PDFs
                if uploaded_file.name.lower().endswith(".zip"):
                    zip_filename = fs.save(uploaded_file.name, uploaded_file)
                    zip_path = fs.path(zip_filename)

                    with zipfile.ZipFile(zip_path, "r") as zip_ref:
                        for member in zip_ref.namelist():
                            if member.lower().endswith(".pdf"):
                                extracted_path = os.path.join(settings.MEDIA_ROOT, member)
                                os.makedirs(os.path.dirname(extracted_path), exist_ok=True)
                                zip_ref.extract(member, settings.MEDIA_ROOT)
                                temp_files.append(extracted_path)

                    fs.delete(zip_filename)  # remove zip after extraction

                # Handle single/multiple PDF files
                elif uploaded_file.name.lower().endswith(".pdf"):
                    saved_file = PdfFile.objects.create(pdf_file=uploaded_file)

                    pdf_filename = fs.save(uploaded_file.name, uploaded_file)

                    pdf_path = fs.path(pdf_filename)
                    temp_files.append(pdf_path)
                else:
                    return render(request, self.template_name, {"error": "Only PDF or ZIP files are allowed."})

                # Process each PDF
                for pdf_file in temp_files:
                    try:

                        extraction_result = read_pdf_with_structure(pdf_file)
                        raw_text = extraction_result["text"]
                        tables = extraction_result.get("tables", [])

                        english_text = _extract_english_from_pdf(raw_text)
                        parsed_data = parse_contract_text_to_json(english_text, tables)
                        source_file_id = saved_file.id if (saved_file and getattr(saved_file, 'pdf_file', None)) else ""
                        parsed_data = {
                            "source_file": source_file_id,
                            **parsed_data
                        }
                        all_results.append({
                            "filename": os.path.basename(pdf_file),
                            "parsed_data": json.dumps(parsed_data, indent=2, ensure_ascii=False),
                            "english_text": english_text,
                            "summary": {
                                "pages": extraction_result.get("pages_count", 0),
                                "tables_count": len(tables),
                                "english_text_length": len(english_text),
                                "extraction_method": extraction_result.get("method", "unknown"),
                                "ocr_used": extraction_result.get("ocr_used", False)
                            }
                        })
                    finally:
                        if os.path.exists(pdf_file):
                            os.remove(pdf_file)

            except Exception as e:
                return render(request, self.template_name, {"error": f"Error processing file {uploaded_file.name}: {str(e)}"})

        # If only one file was uploaded, show it directly
        if len(all_results) == 1:
            return render(request, self.template_name, all_results[0])

        # If multiple, pass all results
        return render(request, self.template_name, {"multiple_results": all_results})







class ContractTableView(View):
    template_name = "contracts/contract_table.html"
    per_page = 50

    def get(self, request):
        # Extract filters
        search_query = request.GET.get("search", "").strip()
        org_filter = request.GET.get("organisation_name", "").strip()
        dept_filter = request.GET.get("department", "").strip()
        ministry_filter = request.GET.get("ministry", "").strip()
        date_from = request.GET.get("date_from", "").strip()
        date_to = request.GET.get("date_to", "").strip()

        # Base queryset with prefetching
        contracts_qs = Contract.objects.select_related(
            "file", "organization_details", "buyer",
            "financial_approval", "paying_authority", "seller", "epbg"
        ).prefetch_related(
            Prefetch('products', queryset=Product.objects.prefetch_related(
                Prefetch('specifications', queryset=ProductSpecification.objects.all()),
                Prefetch('consignees', queryset=ConsigneeDetail.objects.all())
            )),
            Prefetch('terms', queryset=TermsAndCondition.objects.all())
        ).order_by('-generated_date', '-created_at')

        # Apply filters
        if org_filter:
            contracts_qs = contracts_qs.filter(organization_details__organisation_name__icontains=org_filter)
        if dept_filter:
            contracts_qs = contracts_qs.filter(organization_details__department__icontains=dept_filter)
        if ministry_filter:
            contracts_qs = contracts_qs.filter(organization_details__ministry__icontains=ministry_filter)
        if date_from:
            contracts_qs = contracts_qs.filter(generated_date__gte=date_from)
        if date_to:
            contracts_qs = contracts_qs.filter(generated_date__lte=date_to)
        if search_query:
            contracts_qs = contracts_qs.filter(
                Q(contract_no__icontains=search_query) |
                Q(organization_details__organisation_name__icontains=search_query) |
                Q(organization_details__department__icontains=search_query) |
                Q(organization_details__ministry__icontains=search_query) |
                Q(seller__company_name__icontains=search_query) |
                Q(buyer__email__icontains=search_query) |
                Q(products__product_name__icontains=search_query)
            ).distinct()

        # Build comprehensive data structure
        contract_data = []
        for contract in contracts_qs:
            # File info
            source_file = contract.file.pdf_file.url if contract.file and contract.file.pdf_file else ""
            source_filename = os.path.basename(urlparse(source_file).path) if source_file else ""

            # Calculate totals
            total_value = Decimal('0')
            total_qty = 0
            for product in contract.products.all():
                try:
                    total_value += Decimal(product.total_price) if product.total_price else Decimal('0')
                except:
                    pass
                try:
                    total_qty += int(product.ordered_quantity) if product.ordered_quantity else 0
                except:
                    pass

            # Collect specifications
            specifications = []
            for product in contract.products.all():
                for spec in product.specifications.all():
                    specifications.append(f"{spec.category}: {spec.sub_spec} = {spec.value}")

            # Collect consignees
            consignees = []
            for product in contract.products.all():
                for consignee in product.consignees.all():
                    consignees.append(
                        f"{consignee.designation} - {consignee.address} "
                        f"(Qty: {consignee.quantity}, Period: {consignee.delivery_start} to {consignee.delivery_end})"
                    )

            # Collect terms
            terms = [term.clause_text for term in contract.terms.all()]

            contract_data.append({
                # Contract fields
                "contract_no": contract.contract_no,
                "generated_date": contract.generated_date,
                "raw_text": contract.raw_text,
                "source_file": source_file,
                "source_filename": source_filename,

                # Organisation details
                "org_type": contract.organization_details.type if contract.organization_details else "",
                "ministry": contract.organization_details.ministry if contract.organization_details else "",
                "department": contract.organization_details.department if contract.organization_details else "",
                "organisation": contract.organization_details.organisation_name if contract.organization_details else "",
                "office_zone": contract.organization_details.office_zone if contract.organization_details else "",

                # Buyer details
                "buyer_designation": contract.buyer.designation if contract.buyer else "",
                "buyer_contact": contract.buyer.contact_no if contract.buyer else "",
                "buyer_email": contract.buyer.email if contract.buyer else "",
                "buyer_gstin": contract.buyer.gstin if contract.buyer else "",
                "buyer_address": contract.buyer.address if contract.buyer else "",

                # Financial approval
                "ifd_concurrence": "Yes" if contract.financial_approval and contract.financial_approval.ifd_concurrence else "No",
                "admin_approval": contract.financial_approval.admin_approval_designation if contract.financial_approval else "",
                "financial_approval": contract.financial_approval.financial_approval_designation if contract.financial_approval else "",

                # Paying authority
                "paying_role": contract.paying_authority.role if contract.paying_authority else "",
                "payment_mode": contract.paying_authority.payment_mode if contract.paying_authority else "",
                "paying_designation": contract.paying_authority.designation if contract.paying_authority else "",
                "paying_email": contract.paying_authority.email if contract.paying_authority else "",
                "paying_gstin": contract.paying_authority.gstin if contract.paying_authority else "",
                "paying_address": contract.paying_authority.address if contract.paying_authority else "",

                # Seller details
                "gem_seller_id": contract.seller.gem_seller_id if contract.seller else "",
                "seller_company": contract.seller.company_name if contract.seller else "",
                "seller_contact": contract.seller.contact_no if contract.seller else "",
                "seller_email": contract.seller.email if contract.seller else "",
                "seller_address": contract.seller.address if contract.seller else "",
                "msme_reg": contract.seller.msme_registration_number if contract.seller else "",
                "seller_gstin": contract.seller.gstin if contract.seller else "",

                # Products
                "products": [
                    {
                        "name": p.product_name,
                        "brand": p.brand,
                        "type": p.brand_type,
                        "status": p.catalogue_status,
                        "selling_as": p.selling_as,
                        "category": p.category_name_quadrant,
                        "model": p.model,
                        "hsn": p.hsn_code,
                        "quantity": p.ordered_quantity,
                        "unit": p.unit,
                        "unit_price": p.unit_price,
                        "tax": p.tax_bifurcation,
                        "total_price": p.total_price,
                        "note": p.note
                    } for p in contract.products.all()
                ],
                "specifications": specifications,

                # Consignees
                "consignees": consignees,

                # EPBG
                # "epbg_detail": contract.epbg.detail if contract.epbg else "",

                # Terms
                "terms": terms,

                # Calculated values
                "total_value": total_value,
                "total_quantity": total_qty,
            })

        # Export handling
        if request.GET.get("export") in ["excel", "csv"]:
            return self.export_data(contract_data, request.GET.get("export"))

        # Pagination
        paginator = Paginator(contract_data, self.per_page)
        page_number = request.GET.get('page', 1)
        page_obj = paginator.get_page(page_number)

        # Filter options
        org_options = OrganisationDetail.objects.exclude(organisation_name="").values_list("organisation_name",
                                                                                           flat=True).distinct()
        dept_options = OrganisationDetail.objects.exclude(department="").values_list("department", flat=True).distinct()
        ministry_options = OrganisationDetail.objects.exclude(ministry="").values_list("ministry", flat=True).distinct()

        return render(request, self.template_name, {
            "page_obj": page_obj,
            "search_query": search_query,
            "org_filter": org_filter,
            "dept_filter": dept_filter,
            "ministry_filter": ministry_filter,
            "date_from": date_from,
            "date_to": date_to,
            "org_options": org_options,
            "dept_options": dept_options,
            "ministry_options": ministry_options,
        })
    def export_data(self, rows, file_type):
        df = pd.DataFrame(rows)
        if "contract_obj" in df.columns:
            df = df.drop(columns=["contract_obj"])
        if file_type == "excel":
            response = HttpResponse(content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            response["Content-Disposition"] = 'attachment; filename="contracts.xlsx"'
            df.to_excel(response, index=False)
            return response
        elif file_type == "csv":
            response = HttpResponse(content_type="text/csv")
            response["Content-Disposition"] = 'attachment; filename="contracts.csv"'
            df.to_csv(response, index=False)
            return response



class SaveInDb(View):
    def post(self, request):
        """
        POST JSON expects:
        {
           "parsed_data": { ... },   # parsed JSON from your parser
           "english_text": "..."     # optional full text
        }
        """
        if request.method != 'POST':
            return JsonResponse({'success': False, 'message': 'Invalid request method'}, status=405)

        # parse payload
        try:
            payload = json.loads(request.body)
        except Exception as e:
            return JsonResponse({'success': False, 'message': f'Invalid JSON: {str(e)}'}, status=400)

        parsed_data = payload.get('parsed_data') or {}
        # If parsed_data itself is a JSON string, attempt to load it
        if isinstance(parsed_data, str):
            try:
                parsed_data = json.loads(parsed_data)
            except Exception:
                # keep as string/dict fallback
                parsed_data = payload.get('parsed_data') or {}

        english_text = payload.get('english_text') or ''
        pdf_relative_path = payload.get('pdf_relative_path') or payload.get('file_relpath') or ''

        # top-level pieces
        contract_obj = parsed_data.get('contract', {}) or {}
        file_id = parsed_data.get('source_file',{}) or {}
        org_obj = parsed_data.get('organisation', {}) or parsed_data.get('organization', {}) or {}
        buyer_obj = parsed_data.get('buyer', {}) or {}
        print("BUYER IS HERE : ..",buyer_obj)
        seller_obj = parsed_data.get('seller', {}) or {}
        fin_obj = parsed_data.get('financial_approval', {}) or {}
        paying_obj = parsed_data.get('paying_authority', {}) or {}
        products_top = parsed_data.get('products', []) or []
        consignees_top = parsed_data.get('consignees', []) or []
        specs_top = parsed_data.get('specifications', []) or []
        terms_top = parsed_data.get('terms', []) or []
        epbg_top = parsed_data.get('epbg', '') or ''
        tables = parsed_data.get('tables', []) or []

        # table heuristics
        table_extracts = extract_from_tables(tables)

        # Build combined products list (normalized) - ensure string fields never None
        combined_products = []
        for p in products_top:
            if isinstance(p, dict):
                combined_products.append({
                    'product_name': safe_str(p.get('product_name') or p.get('name') or ''),
                    'ordered_quantity': parse_int(p.get('ordered_quantity')),
                    'unit_price': parse_decimal(p.get('unit_price')),
                    'total_price': parse_decimal(p.get('total_price')),
                    'category_name_quadrant': safe_str(p.get('category_name_quadrant') or p.get('category')),
                    'hsn_code': safe_str(p.get('hsn_code') or p.get('hsn')),
                    'note': safe_str(p.get('note') or '')
                })
            else:
                # plain string product
                combined_products.append({
                    'product_name': safe_str(p),
                    'ordered_quantity': None,
                    'unit_price': None,
                    'total_price': None,
                    'category_name_quadrant': '',
                    'hsn_code': '',
                    'note': ''
                })

        # add table-derived products (heuristic)
        for p in table_extracts.get('products', []):
            combined_products.append({
                'product_name': safe_str(p.get('product_name')),
                'ordered_quantity': p.get('ordered_quantity'),
                'unit_price': p.get('unit_price'),
                'total_price': p.get('total_price'),
                'category_name_quadrant': safe_str(p.get('category_name_quadrant', '')),
                'hsn_code': safe_str(p.get('hsn', '')),
                'note': ''
            })

        # Build consignees list normalized
        combined_consignees = []
        for c in consignees_top:
            combined_consignees.append({
                's_no': c.get('s_no'),
                'designation': safe_str(c.get('designation')),
                'email': extract_email(c.get('email') or c.get('email id') or c.get('email id :')),
                'contact': extract_phone(c.get('contact') or c.get('contact_no')),
                'gstin': safe_str(c.get('gstin')),
                'address': safe_str(c.get('address') or c.get('adres') or ''),
                'lot_no': safe_str(c.get('lot_no')),
                'quantity': parse_int(c.get('quantity')),
                'delivery_start': parse_date(c.get('delivery_start')),
                'delivery_end': parse_date(c.get('delivery_end')),
                'delivery_to': safe_str(c.get('delivery_to') or '')
            })
        for c in table_extracts.get('consignees', []):
            # table-derived consignees likely already dict-like
            combined_consignees.append({
                's_no': c.get('s_no'),
                'designation': safe_str(c.get('designation')),
                'email': extract_email(buyer_obj.get('email')),
                'contact': extract_phone(c.get('contact')),
                'gstin': safe_str(c.get('gstin')),
                'address': safe_str(c.get('address')),
                'lot_no': safe_str(c.get('lot_no')),
                'quantity': c.get('quantity'),
                'delivery_start': c.get('delivery_start'),
                'delivery_end': c.get('delivery_end'),
                'delivery_to': safe_str(c.get('delivery_to'))
            })

        # Specifications combined and normalized
        combined_specs = []
        for s in specs_top:
            combined_specs.append({
                'category': safe_str(s.get('category')),
                'sub_spec': safe_str(s.get('sub_spec')),
                'value': safe_str(s.get('value'))
            })
        for s in table_extracts.get('specifications', []):
            combined_specs.append({
                'category': safe_str(s.get('category')),
                'sub_spec': safe_str(s.get('sub_spec')),
                'value': safe_str(s.get('value'))
            })

        # EPBG & Terms
        epbg_text = safe_str(epbg_top) or safe_str(table_extracts.get('epbg') or '')
        terms_combined = [safe_str(t) for t in terms_top if t] + [safe_str(t) for t in table_extracts.get('terms', []) if t]

        created = {
            'contract': False, 'org': False, 'buyer': False, 'seller': False,
            'financial': False, 'paying': False, 'products': 0, 'specs': 0, 'consignees': 0,
            'epbg': False, 'terms': 0
        }

        try:
            with transaction.atomic():
                # Contract
                contract_no = safe_str(contract_obj.get('contract_no') or contract_obj.get('contract_no', '') or contract_obj.get('contract_no', ''))
                if not contract_no:
                    return JsonResponse({'success': False, 'message': 'No contract number found in parsed data'}, status=400)
                generated_date = parse_date(contract_obj.get('generated_date') or contract_obj.get('generated_date', ''))

                contract, created_flag = Contract.objects.get_or_create(
                    contract_no=contract_no,
                    file_id =file_id,
                    defaults={'generated_date': generated_date, 'raw_text': english_text}
                )
                created['contract'] = created_flag
                if not created_flag:
                    if generated_date:
                        contract.generated_date = generated_date
                    if english_text:
                        contract.raw_text = english_text
                    contract.save()

                # Attach PDF file path if provided and exists
                if pdf_relative_path:
                    try:
                        abs_path = os.path.join(settings.MEDIA_ROOT, pdf_relative_path)
                        if os.path.exists(abs_path):
                            # Assign by name so Django FileField serves it
                            contract.pdf_file.name = pdf_relative_path.replace('\\', '/')
                            contract.save(update_fields=['pdf_file'])
                    except Exception:
                        pass

                # Compute and store contract embedding if model available
                embedder = get_embedder()
                if embedder is not None and english_text:
                    try:
                        combo = ' | '.join(filter(None, [
                            contract.contract_no,
                            contract.generated_date.isoformat() if contract.generated_date else '',
                            english_text
                        ]))
                        vec = embedder.encode([combo], normalize_embeddings=True)
                        contract.embedding = vec[0].tolist()
                        contract.save(update_fields=['embedding'])
                    except Exception:
                        pass

                # OrganisationDetail
                org_defaults = {
                    'type': safe_str(org_obj.get('type')),
                    'ministry': safe_str(org_obj.get('ministry')),
                    'department': safe_str(org_obj.get('department')),
                    'organisation_name': safe_str(org_obj.get('organisation_name') or org_obj.get('organisation') or org_obj.get('organisation_name')),
                    'office_zone': safe_str(org_obj.get('office') or org_obj.get('office_zone') or org_obj.get('ofice') or '')
                }
                org_detail, org_created = OrganisationDetail.objects.update_or_create(contract=contract, defaults=org_defaults)
                created['org'] = org_created

                # BuyerDetail
                buyer_defaults = {
                    'designation': safe_str(buyer_obj.get('designation')),
                    'contact_no': extract_phone(buyer_obj.get('contact_no') or buyer_obj.get('contact') or buyer_obj.get('contact no')),
                    'email': extract_email(buyer_obj.get('email') or buyer_obj.get('email id') or buyer_obj.get('email id :')),
                    'gstin': safe_str(buyer_obj.get('gstin')),
                    'address': safe_str(buyer_obj.get('address'))
                }
                buyer_detail, buyer_created = BuyerDetail.objects.update_or_create(contract=contract, defaults=buyer_defaults)
                created['buyer'] = buyer_created

                # FinancialApproval
                financial_defaults = {
                    'ifd_concurrence': bool(fin_obj.get('ifd_concurrence') or fin_obj.get('ifd') or False),
                    'admin_approval_designation': safe_str(fin_obj.get('admin_approval_designation')),
                    'financial_approval_designation': safe_str(fin_obj.get('financial_approval_designation'))
                }
                fin_obj_db, fin_created = FinancialApproval.objects.update_or_create(contract=contract, defaults=financial_defaults)
                created['financial'] = fin_created

                # PayingAuthority
                paying_defaults = {
                    'role': safe_str(paying_obj.get('role')),
                    'payment_mode': safe_str(paying_obj.get('payment_mode')),
                    'designation': safe_str(paying_obj.get('designation')),
                    'email': extract_email(paying_obj.get('email')),
                    'gstin': safe_str(paying_obj.get('gstin')),
                    'address': safe_str(paying_obj.get('address'))
                }
                paying_db, paying_created = PayingAuthority.objects.update_or_create(contract=contract, defaults=paying_defaults)
                created['paying'] = paying_created

                # SellerDetail
                seller_defaults = {
                    'gem_seller_id': safe_str(seller_obj.get('gem_seller_id') or seller_obj.get('gem_seler_id')),
                    'company_name': safe_str(seller_obj.get('company_name') or seller_obj.get('seller_name') or seller_obj.get('company')),
                    'contact_no': extract_phone(seller_obj.get('contact_no') or seller_obj.get('contact')),
                    'email': extract_email(seller_obj.get('email')),
                    'address': safe_str(seller_obj.get('address')),
                    'msme_registration_number': safe_str(seller_obj.get('msme_registration_number')),
                    'gstin': safe_str(seller_obj.get('gstin')),
                }
                seller_db, seller_created = SellerDetail.objects.update_or_create(contract=contract, defaults=seller_defaults)
                created['seller'] = seller_created

                # Products + Specs + Consignees
                product_map = {}
                for p in combined_products:
                    pname = safe_str(p.get('product_name') or 'Unknown Product')
                    prod_defaults = {
                        'brand': safe_str(p.get('brand') or ''),
                        'brand_type': safe_str(p.get('brand_type') or ''),
                        'catalogue_status': safe_str(p.get('catalogue_status') or ''),
                        'selling_as': safe_str(p.get('selling_as') or ''),
                        'category_name_quadrant': safe_str(p.get('category_name_quadrant') or p.get('category') or ''),
                        'model': safe_str(p.get('model') or ''),
                        # ensure NOT NULL charfields are empty-string instead of None
                        'hsn_code': safe_str(p.get('hsn_code') or p.get('hsn') or ''),
                        'ordered_quantity': p.get('ordered_quantity'),
                        'unit': safe_str(p.get('unit') or ''),
                        'unit_price': p.get('unit_price'),
                        'tax_bifurcation': p.get('tax_bifurcation'),
                        'total_price': p.get('total_price'),
                        'note': safe_str(p.get('note') or '')
                    }
                    prod_obj, prod_created = Product.objects.update_or_create(contract=contract, product_name=pname, defaults=prod_defaults)
                    product_map[pname] = prod_obj
                    created['products'] += 1 if prod_created else 0

                    for s in combined_specs:
                        ProductSpecification.objects.update_or_create(
                            product=prod_obj,
                            category=s.get('category', ''),
                            sub_spec=s.get('sub_spec', ''),
                            defaults={'value': safe_str(s.get('value', ''))}
                        )
                        created['specs'] += 1

                    # Compute and store product embedding if model available
                    embedder = get_embedder()
                    if embedder is not None:
                        try:
                            pcombo = ' | '.join(filter(None, [
                                prod_obj.product_name,
                                prod_obj.category_name_quadrant,
                                prod_obj.hsn_code,
                                prod_obj.note
                            ]))
                            if pcombo:
                                pvec = embedder.encode([pcombo], normalize_embeddings=True)
                                prod_obj.embedding = pvec[0].tolist()
                                prod_obj.save(update_fields=['embedding'])
                        except Exception:
                            pass
                if not product_map and parsed_data.get('products'):
                    for p in parsed_data.get('products'):
                        pname = safe_str(p if isinstance(p, str) else p.get('product_name', 'Unknown'))
                        prod_obj, _ = Product.objects.get_or_create(contract=contract, product_name=pname, defaults={'hsn_code': ''})
                        product_map[pname] = prod_obj

                for c in combined_consignees:
                    linked_product = None
                    p_name = c.get('product_name') or ''
                    if p_name:
                        p_name = safe_str(p_name)
                        linked_product = product_map.get(p_name)
                    if not linked_product and len(product_map) == 1:
                        linked_product = next(iter(product_map.values()))
                    if not linked_product:
                        linked_product, _ = Product.objects.get_or_create(contract=contract, product_name='(unspecified)', defaults={'hsn_code': ''})

                    consignee_defaults = {
                        'designation': safe_str(c.get('designation')),
                        'email': extract_email(c.get('email') or ''),
                        'contact': extract_phone(c.get('contact') or ''),
                        'gstin': safe_str(c.get('gstin')),
                        'address': safe_str(c.get('address')),
                        'lot_no': safe_str(c.get('lot_no')),
                        'quantity': parse_int(c.get('quantity')),
                        'delivery_start': c.get('delivery_start') if isinstance(c.get('delivery_start'), (datetime,)) else c.get('delivery_start'),
                        'delivery_end': c.get('delivery_end') if isinstance(c.get('delivery_end'), (datetime,)) else c.get('delivery_end'),
                        'delivery_to': safe_str(c.get('delivery_to') or '')
                    }
                    ConsigneeDetail.objects.update_or_create(
                        product=linked_product,
                        s_no=c.get('s_no'),
                        defaults=consignee_defaults
                    )
                    created['consignees'] += 1

                # EPBG
                if epbg_text:
                    epbg_db, epbg_created = EPBGDetail.objects.update_or_create(contract=contract, defaults={'detail': safe_str(epbg_text)})
                    created['epbg'] = epbg_created

                # Terms
                for t in terms_combined:
                    if not t:
                        continue
                    TermsAndCondition.objects.get_or_create(contract=contract, clause_text=t)
                    created['terms'] += 1

        except Exception as exc:
            return JsonResponse({'success': False, 'message': f'Error saving to database: {str(exc)}'}, status=500)

        return JsonResponse({'success': True, 'message': f'Contract {contract.contract_no} saved/updated', 'created': created})



@method_decorator(ensure_csrf_cookie, name='dispatch')
class SemanticSearchView(View):
    template_name = "contracts/search.html"

    def generate_clean_summary(self, text, query):
        print("TEXT : ",text)
        """Generate a clean, query-focused summary using AI transformers"""
        # First, try to answer the query directly
        try:
            # Initialize QA pipeline
            qa_pipeline = pipeline(
                "question-answering",
                model="deepset/roberta-base-squad2",
                tokenizer="deepset/roberta-base-squad2"
            )

            # Try to extract a direct answer
            answer = qa_pipeline(question=query, context=text[:3000])  # Limit context
            if answer['score'] > 0.1:  # Minimum confidence threshold
                return answer['answer']
        except Exception:
            pass

        # If QA fails, generate an extractive summary focused on query terms
        try:
            # Clean text by removing common PDF artifacts
            text = re.sub(r'\s+', ' ', text)  # Collapse whitespace
            text = re.sub(r'[^\w\s.,;:!?()-]', '', text)  # Remove special chars

            # Use extractive summarization focused on query terms
            from summa import keywords
            from summa.summarizer import summarize

            # Boost query terms in importance
            query_keywords = " ".join(set(query.split()))
            boosted_text = f"{query_keywords}. {text}"

            summary = summarize(
                boosted_text,
                ratio=0.2,
                words=100,
                split=True
            )

            if summary:
                return " ".join(summary)
        except Exception:
            pass

        # Fallback: extract sentences containing query terms
        sentences = re.split(r'(?<=[.!?])\s+', text)
        query_terms = query.lower().split()
        relevant_sentences = [
                                 s for s in sentences
                                 if any(term in s.lower() for term in query_terms)
                             ][:3]

        return " ".join(relevant_sentences) or "No relevant information found"

    def clean_raw_text(self,text):
        """Clean and normalize raw text"""
        if not text:
            return ""

        # Remove common PDF artifacts
        text = re.sub(r'\s+', ' ', text)  # Collapse whitespace
        text = re.sub(r'[^\w\s.,;:!?()-]', '', text)  # Remove special chars
        text = re.sub(r'\bPage \d+\b', '', text)  # Remove page numbers

        lines = text.split('.')
        unique_lines = []
        seen = set()
        for line in lines:
            clean_line = line.strip()
            if clean_line and clean_line not in seen:
                unique_lines.append(clean_line)
                seen.add(clean_line)

        return '. '.join(unique_lines)
    def get_model(self):
        if SentenceTransformer is None:
            return None
        # Cache on class
        if not hasattr(self.__class__, "_embedder"):
            try:
                self.__class__._embedder = SentenceTransformer('all-MiniLM-L6-v2')
            except Exception:
                self.__class__._embedder = None
        return getattr(self.__class__, "_embedder", None)

    def get(self, request):
        return render(request, self.template_name, {"query": request.GET.get("q", "")})

    def post(self, request):
        try:
            body = json.loads(request.body or '{}')
        except Exception:
            body = {}
        query = body.get('query') or request.POST.get('query') or ''
        top_k = int(body.get('top_k') or request.POST.get('top_k') or 5)

        if not query:
            return JsonResponse({"success": False, "message": "Empty query"}, status=400)

        model = self.get_model()
        if model is None or np is None:
            # Fallback: keyword search
            qs = (
                Contract.objects.select_related('organization_details', 'buyer', 'seller')
                .filter(
                    Q(contract_no__icontains=query)
                    | Q(raw_text__icontains=query)
                    | Q(organization_details__organisation_name__icontains=query)
                    | Q(organization_details__department__icontains=query)
                    | Q(organization_details__ministry__icontains=query)
                    | Q(seller__company_name__icontains=query)
                    | Q(buyer__email__icontains=query)
                )
                .only('id', 'contract_no', 'generated_date')
                .distinct()
            )
            results = [
                {
                    'raw_text':c.raw_text,
                    'contract_no': c.contract_no,
                    'generated_date': c.generated_date.isoformat() if c.generated_date else '',
                    'score': 0.0,
                }
                for c in qs[:top_k]
            ]
            top_summary = ""

            if results:
                print("HELLO ...",results)
                top_c = qs.first()
                top_summary = self.generate_clean_summary(self.clean_raw_text(results[0].get('raw_text')))

            summary =top_summary

            print("SUMMERY : ",summary)
            return JsonResponse({"success": True, "results": results, "summary": summary, "top_summary": top_summary})

        # Optional: parse date in query to bias ranking
        wanted_date = None
        try:
            mdate = re.search(r'(\d{4}-\d{1,2}-\d{1,2}|\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4})', query)
            if mdate:
                txt = mdate.group(0)
                # normalize to YYYY-MM-DD
                if '-' in txt and len(txt.split('-')[0]) == 4:
                    wanted_date = datetime.fromisoformat(txt).date()
                else:
                    parts = re.split(r'[\-/]', txt)
                    if len(parts[-1]) == 2:
                        parts[-1] = '20' + parts[-1]
                    d, m, y = [int(x) for x in parts]
                    wanted_date = datetime(y, m, d).date()
        except Exception:
            wanted_date = None

        # Build corpus of searchable texts with references
        corpus = []
        refs = []
        for c in Contract.objects.all().select_related('buyer', 'seller', 'organization_details'):
            parts = [
                c.contract_no or '',
                (c.generated_date.isoformat() if c.generated_date else ''),
                getattr(c.organization_details, 'organisation_name', '') if hasattr(c, 'organization_details') else '',
                getattr(c.organization_details, 'department', '') if hasattr(c, 'organization_details') else '',
                getattr(c.seller, 'company_name', '') if hasattr(c, 'seller') else '',
                getattr(c.buyer, 'email', '') if hasattr(c, 'buyer') else '',
                c.raw_text or ''
            ]
            corpus.append(' | '.join([p for p in parts if p]))
            refs.append({
                'id': c.id,
                'contract_no': c.contract_no,
                'generated_date': c.generated_date.isoformat() if c.generated_date else '',
                'date_obj': c.generated_date,
                'type': 'contract',
            })

        # Add product names and notes as separate entries linked to their contract
        for p in (
            Product.objects
            .select_related('contract')
            .only(
                'id', 'contract_id', 'product_name', 'note', 'hsn_code', 'category_name_quadrant',
                'contract__contract_no', 'contract__generated_date'
            )
        ):
            parts = [p.product_name or '', p.note or '', p.hsn_code or '', p.category_name_quadrant or '']
            corpus.append(' | '.join([x for x in parts if x]))
            refs.append({
                'id': p.contract_id,
                'contract_no': p.contract.contract_no if p.contract_id else '',
                'generated_date': p.contract.generated_date.isoformat() if p.contract and p.contract.generated_date else '',
                'date_obj': p.contract.generated_date if p.contract else None,
                'type': 'product',
            })

        if not corpus:
            return JsonResponse({"success": True, "results": [], "summary": "No data indexed yet"})

        # Use precomputed embeddings when available for contracts/products
        query_vec = model.encode([query], normalize_embeddings=True)
        # Compute corpus embeddings in batches for memory safety
        batch_size = 256
        corpus_vecs = []
        for i in range(0, len(corpus), batch_size):
            batch = corpus[i:i+batch_size]
            vecs = model.encode(batch, normalize_embeddings=True)
            corpus_vecs.append(vecs)
        corpus_vecs = np.vstack(corpus_vecs)
        # cosine similarity
        sims = (query_vec @ corpus_vecs.T)[0]

        # Apply date bias if query contained a date
        if wanted_date is not None:
            bias = np.zeros_like(sims)
            for i, ref in enumerate(refs):
                d = ref.get('date_obj')
                if not d:
                    continue
                if d == wanted_date:
                    bias[i] = 0.2
                elif d.year == wanted_date.year and d.month == wanted_date.month:
                    bias[i] = 0.1
                else:
                    delta = abs((d - wanted_date).days)
                    if delta <= 14:
                        bias[i] = max(0.0, 0.1 * (1 - delta / 14.0))
            sims = sims + bias
        top_idx = np.argsort(-sims)[:top_k]

        seen_contracts = set()
        results = []
        for i in top_idx.tolist():
            ref = refs[i]
            key = ref['id']
            if key in seen_contracts:
                continue
            seen_contracts.add(key)
            results.append({
                'contract_no': ref['contract_no'],
                'generated_date': ref['generated_date'],
                'score': float(sims[i])
            })
            if len(results) >= top_k:
                break

        # short summary and top summary text
        top_summary = ""
        if results:
            summary = f"Top match: {results[0]['contract_no']} dated {results[0]['generated_date']} (score {results[0]['score']:.3f})"
            try:
                top_ref = refs[top_idx.tolist()[0]]
                top_contract = Contract.objects.filter(id=top_ref['id']).first()
                if top_contract and top_contract.raw_text:
                    cleaned_text = self.clean_raw_text(top_contract.raw_text)
                    top_summary = self.generate_clean_summary(cleaned_text, query)
            except Exception as e:
                print(f"Error generating semantic summary: {str(e)}")
                # Fallback to sentence extraction
                if top_contract and top_contract.raw_text:
                    text = top_contract.raw_text
                    sentences = re.split(r'(?<=[.!?])\s+', text)
                    query_terms = query.lower().split()
                    relevant_sentences = [
                                             s for s in sentences
                                             if any(term in s.lower() for term in query_terms)
                                         ][:3]
                    top_summary = " ".join(relevant_sentences) or "No relevant information found"
        else:
            summary = "No relevant contracts found"
        print("SUMMARY : ",summary)
        return JsonResponse({
            "success": True,
            "results": results,
            "summary": top_summary,
            "top_summary": top_summary
        })