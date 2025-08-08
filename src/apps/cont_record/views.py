# contracts/views.py
from django.views.generic import TemplateView
from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import JsonResponse, HttpResponse
from django.core.files.storage import FileSystemStorage
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime
import json
import pandas as pd
from io import BytesIO
import re

from src.utils.contract_parsers import parse_contract_text_to_json
from src.utils.extract_text import read_pdf_with_structure, _extract_english_from_pdf
from .models import (
    Contract, OrganisationDetail, BuyerDetail, FinancialApproval, 
    PayingAuthority, SellerDetail, Product, ProductSpecification, 
    ConsigneeDetail, EPBGDetail, TermsAndCondition
)


def upload_pdfs(request):
    if request.method == 'POST':
        uploaded_file = request.FILES.get('pdf_file')
        
        if uploaded_file and uploaded_file.name.endswith('.pdf'):
            # Save the uploaded file temporarily
            fs = FileSystemStorage()
            filename = fs.save(uploaded_file.name, uploaded_file)
            file_path = fs.path(filename)
            
            try:
                # Extract text and tables from PDF
                extraction_result = read_pdf_with_structure(file_path)
                raw_text = extraction_result['text']
                tables = extraction_result.get('tables', [])
                
                # Extract English-only text
                english_text = _extract_english_from_pdf(raw_text)
                
                # Parse the English text to JSON
                parsed_data = parse_contract_text_to_json(english_text, tables)
                
                # Prepare context for template
                context = {
                    'uploaded_file': uploaded_file,
                    'extraction_result': extraction_result,
                    'parsed_data': json.dumps(parsed_data, indent=2, ensure_ascii=False),
                    'english_text': english_text,  # Show FULL English text
                    'english_text_length': len(english_text),
                    'summary': {
                        'pages': extraction_result.get('pages_count', 0),
                        'tables_count': len(tables),
                        'english_text_length': len(english_text),
                        'extraction_method': extraction_result.get('method', 'unknown'),
                        'ocr_used': extraction_result.get('ocr_used', False)
                    }
                }
                
                # Clean up the temporary file
                fs.delete(filename)
                
                return render(request, 'contracts/upload_pdfs.html', context)
                
            except Exception as e:
                # Clean up the temporary file in case of error
                fs.delete(filename)
                context = {
                    'error': f'Error processing PDF: {str(e)}'
                }
                return render(request, 'contracts/upload_pdfs.html', context)
        else:
            context = {
                'error': 'Please upload a valid PDF file.'
            }
            return render(request, 'contracts/upload_pdfs.html', context)
    
    return render(request, 'contracts/upload_pdfs.html')


@csrf_exempt
def save_to_database(request):
    """Save extracted JSON data to database"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            parsed_data = data.get('parsed_data', {})
            
            # Extract contract data
            contract_data = parsed_data.get('contract', {})
            contract_no = contract_data.get('contract_no', '')
            
            if not contract_no:
                return JsonResponse({
                    'success': False,
                    'message': 'Contract number is required'
                })
            
            # Check if contract already exists
            contract, created = Contract.objects.get_or_create(
                contract_no=contract_no,
                defaults={
                    'generated_date': _parse_date(contract_data.get('generated_date')),
                    'raw_text': data.get('english_text', '')
                }
            )
            
            # Update contract if it already exists
            if not created:
                contract.generated_date = _parse_date(contract_data.get('generated_date'))
                contract.raw_text = data.get('english_text', '')
                contract.save()
            
            # Save Organisation Details
            org_data = parsed_data.get('organisation', {})
            org_detail, _ = OrganisationDetail.objects.get_or_create(
                contract=contract,
                defaults={
                    'type': org_data.get('type', ''),
                    'ministry': org_data.get('ministry', ''),
                    'department': org_data.get('department', ''),
                    'organisation_name': org_data.get('organisation_name', ''),
                    'office_zone': org_data.get('office', '')
                }
            )
            
            # Save Buyer Details
            buyer_data = parsed_data.get('buyer', {})
            buyer_detail, _ = BuyerDetail.objects.get_or_create(
                contract=contract,
                defaults={
                    'designation': buyer_data.get('designation', ''),
                    'contact_no': buyer_data.get('contact_no', ''),
                    'email': buyer_data.get('email', ''),
                    'gstin': buyer_data.get('gstin', ''),
                    'address': buyer_data.get('address', '')
                }
            )
            
            # Save Financial Approval
            financial_data = parsed_data.get('financial_approval', {})
            financial_approval, _ = FinancialApproval.objects.get_or_create(
                contract=contract,
                defaults={
                    'ifd_concurrence': financial_data.get('ifd_concurrence', False),
                    'admin_approval_designation': financial_data.get('admin_approval_designation', ''),
                    'financial_approval_designation': financial_data.get('financial_approval_designation', '')
                }
            )
            
            # Save Paying Authority
            paying_data = parsed_data.get('paying_authority', {})
            paying_authority, _ = PayingAuthority.objects.get_or_create(
                contract=contract,
                defaults={
                    'role': paying_data.get('role', ''),
                    'payment_mode': paying_data.get('payment_mode', ''),
                    'designation': paying_data.get('designation', ''),
                    'email': paying_data.get('email', ''),
                    'gstin': paying_data.get('gstin', ''),
                    'address': paying_data.get('address', '')
                }
            )
            
            # Save Seller Details
            seller_data = parsed_data.get('seller', {})
            seller_detail, _ = SellerDetail.objects.get_or_create(
                contract=contract,
                defaults={
                    'gem_seller_id': seller_data.get('gem_seller_id', ''),
                    'company_name': seller_data.get('seller_name', ''),
                    'contact_no': seller_data.get('contact_no', ''),
                    'email': seller_data.get('email', ''),
                    'address': seller_data.get('address', ''),
                    'msme_registration_number': seller_data.get('msme_registration_number', ''),
                    'gstin': seller_data.get('gstin', '')
                }
            )
            
            # Save Products
            products_data = parsed_data.get('products', [])
            for product_data in products_data:
                product, _ = Product.objects.get_or_create(
                    contract=contract,
                    product_name=product_data.get('product_name', 'Unknown Product'),
                    defaults={
                        'brand': product_data.get('brand', ''),
                        'brand_type': product_data.get('brand_type', ''),
                        'catalogue_status': product_data.get('catalogue_status', ''),
                        'selling_as': product_data.get('selling_as', ''),
                        'category_name_quadrant': product_data.get('category_name_quadrant', ''),
                        'model': product_data.get('model', ''),
                        'hsn_code': product_data.get('hsn_code', ''),
                        'ordered_quantity': _parse_int(product_data.get('ordered_quantity')),
                        'unit': product_data.get('unit', ''),
                        'unit_price': _parse_decimal(product_data.get('unit_price')),
                        'tax_bifurcation': _parse_decimal(product_data.get('tax_bifurcation')),
                        'total_price': _parse_decimal(product_data.get('total_price')),
                        'note': product_data.get('note', '')
                    }
                )
                
                # Save Product Specifications
                specs_data = parsed_data.get('specifications', [])
                for spec_data in specs_data:
                    ProductSpecification.objects.get_or_create(
                        product=product,
                        category=spec_data.get('category', ''),
                        sub_spec=spec_data.get('sub_spec', ''),
                        value=spec_data.get('value', '')
                    )
                
                # Save Consignee Details
                consignees_data = parsed_data.get('consignees', [])
                for consignee_data in consignees_data:
                    ConsigneeDetail.objects.get_or_create(
                        product=product,
                        s_no=consignee_data.get('s_no'),
                        defaults={
                            'designation': consignee_data.get('designation', ''),
                            'email': consignee_data.get('email', ''),
                            'contact': consignee_data.get('contact', ''),
                            'gstin': consignee_data.get('gstin', ''),
                            'address': consignee_data.get('address', ''),
                            'lot_no': consignee_data.get('lot_no', ''),
                            'quantity': _parse_int(consignee_data.get('quantity')),
                            'delivery_start': _parse_date(consignee_data.get('delivery_start')),
                            'delivery_end': _parse_date(consignee_data.get('delivery_end')),
                            'delivery_to': consignee_data.get('delivery_to', '')
                        }
                    )
            
            # Save EPBG Details
            epbg_data = parsed_data.get('epbg', '')
            if epbg_data:
                EPBGDetail.objects.get_or_create(
                    contract=contract,
                    defaults={'detail': epbg_data}
                )
            
            # Save Terms and Conditions
            terms_data = parsed_data.get('terms', [])
            for term_text in terms_data:
                TermsAndCondition.objects.get_or_create(
                    contract=contract,
                    clause_text=term_text
                )
            
            return JsonResponse({
                'success': True,
                'message': f'Contract {contract_no} saved successfully!',
                'contract_id': contract.id
            })
            
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'Error saving to database: {str(e)}'
            })
    
    return JsonResponse({
        'success': False,
        'message': 'Invalid request method'
    })


@csrf_exempt
def export_to_excel(request):
    """Export extracted data to Excel file"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            parsed_data = data.get('parsed_data', {})
            
            # Create Excel writer
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                
                # Contract Information Sheet
                contract_data = parsed_data.get('contract', {})
                contract_df = pd.DataFrame([{
                    'Contract Number': contract_data.get('contract_no', ''),
                    'Generated Date': contract_data.get('generated_date', ''),
                }])
                contract_df.to_excel(writer, sheet_name='Contract Info', index=False)
                
                # Organization Details Sheet
                org_data = parsed_data.get('organisation', {})
                org_df = pd.DataFrame([{
                    'Type': org_data.get('type', ''),
                    'Ministry': org_data.get('ministry', ''),
                    'Department': org_data.get('department', ''),
                    'Organization Name': org_data.get('organisation_name', ''),
                    'Office': org_data.get('office', '')
                }])
                org_df.to_excel(writer, sheet_name='Organization Details', index=False)
                
                # Buyer Details Sheet
                buyer_data = parsed_data.get('buyer', {})
                buyer_df = pd.DataFrame([{
                    'Designation': buyer_data.get('designation', ''),
                    'Contact Number': buyer_data.get('contact_no', ''),
                    'Email': buyer_data.get('email', ''),
                    'GSTIN': buyer_data.get('gstin', ''),
                    'Address': buyer_data.get('address', '')
                }])
                buyer_df.to_excel(writer, sheet_name='Buyer Details', index=False)
                
                # Financial Approval Sheet
                financial_data = parsed_data.get('financial_approval', {})
                financial_df = pd.DataFrame([{
                    'IFD Concurrence': financial_data.get('ifd_concurrence', ''),
                    'Admin Approval Designation': financial_data.get('admin_approval_designation', ''),
                    'Financial Approval Designation': financial_data.get('financial_approval_designation', '')
                }])
                financial_df.to_excel(writer, sheet_name='Financial Approval', index=False)
                
                # Paying Authority Sheet
                paying_data = parsed_data.get('paying_authority', {})
                paying_df = pd.DataFrame([{
                    'Role': paying_data.get('role', ''),
                    'Payment Mode': paying_data.get('payment_mode', ''),
                    'Designation': paying_data.get('designation', ''),
                    'Email': paying_data.get('email', ''),
                    'GSTIN': paying_data.get('gstin', ''),
                    'Address': paying_data.get('address', '')
                }])
                paying_df.to_excel(writer, sheet_name='Paying Authority', index=False)
                
                # Seller Details Sheet
                seller_data = parsed_data.get('seller', {})
                seller_df = pd.DataFrame([{
                    'GEM Seller ID': seller_data.get('gem_seller_id', ''),
                    'Company Name': seller_data.get('seller_name', ''),
                    'Contact Number': seller_data.get('contact_no', ''),
                    'Email': seller_data.get('email', ''),
                    'Address': seller_data.get('address', ''),
                    'MSME Registration': seller_data.get('msme_registration_number', ''),
                    'GSTIN': seller_data.get('gstin', '')
                }])
                seller_df.to_excel(writer, sheet_name='Seller Details', index=False)
                
                # Products Sheet
                products_data = parsed_data.get('products', [])
                if products_data:
                    products_df = pd.DataFrame(products_data)
                    products_df.to_excel(writer, sheet_name='Products', index=False)
                else:
                    # Create empty products sheet
                    pd.DataFrame(columns=['Product Name', 'Brand', 'Quantity', 'Unit Price', 'Total Price']).to_excel(
                        writer, sheet_name='Products', index=False
                    )
                
                # Specifications Sheet
                specs_data = parsed_data.get('specifications', [])
                if specs_data:
                    specs_df = pd.DataFrame(specs_data)
                    specs_df.to_excel(writer, sheet_name='Specifications', index=False)
                else:
                    # Create empty specifications sheet
                    pd.DataFrame(columns=['Category', 'Sub Spec', 'Value']).to_excel(
                        writer, sheet_name='Specifications', index=False
                    )
                
                # Consignees Sheet
                consignees_data = parsed_data.get('consignees', [])
                if consignees_data:
                    consignees_df = pd.DataFrame(consignees_data)
                    consignees_df.to_excel(writer, sheet_name='Consignees', index=False)
                else:
                    # Create empty consignees sheet
                    pd.DataFrame(columns=['Designation', 'Email', 'Contact', 'Address', 'Delivery To']).to_excel(
                        writer, sheet_name='Consignees', index=False
                    )
                
                # EPBG Details Sheet
                epbg_data = parsed_data.get('epbg', '')
                epbg_df = pd.DataFrame([{'EPBG Details': epbg_data}])
                epbg_df.to_excel(writer, sheet_name='EPBG Details', index=False)
                
                # Terms and Conditions Sheet
                terms_data = parsed_data.get('terms', [])
                if terms_data:
                    terms_df = pd.DataFrame({'Terms and Conditions': terms_data})
                    terms_df.to_excel(writer, sheet_name='Terms & Conditions', index=False)
                else:
                    # Create empty terms sheet
                    pd.DataFrame(columns=['Terms and Conditions']).to_excel(
                        writer, sheet_name='Terms & Conditions', index=False
                    )
                
                # Raw Data Sheet
                raw_data = {
                    'Extracted English Text': [data.get('english_text', '')],
                    'JSON Data': [json.dumps(parsed_data, indent=2)]
                }
                raw_df = pd.DataFrame(raw_data)
                raw_df.to_excel(writer, sheet_name='Raw Data', index=False)
            
            # Prepare response
            output.seek(0)
            contract_no = parsed_data.get('contract', {}).get('contract_no', 'unknown')
            filename = f"contract_data_{contract_no}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            response = HttpResponse(
                output.read(),
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            
            return response
            
        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': f'Error exporting to Excel: {str(e)}'
            })
    
    return JsonResponse({
        'success': False,
        'message': 'Invalid request method'
    })


def _parse_date(date_str):
    """Parse date string to Date object"""
    if not date_str:
        return None
    
    try:
        # Try different date formats
        date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d']
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        return None
    except:
        return None


def _parse_int(value):
    """Parse string to integer"""
    if not value:
        return None
    
    try:
        # Remove non-numeric characters except decimal point
        cleaned = re.sub(r'[^\d.]', '', str(value))
        return int(float(cleaned))
    except:
        return None


def _parse_decimal(value):
    """Parse string to Decimal"""
    if not value:
        return None
    
    try:
        # Remove non-numeric characters except decimal point
        cleaned = re.sub(r'[^\d.]', '', str(value))
        return float(cleaned)
    except:
        return None


def data_details(request):
    """Display detailed data view with navigation"""
    return render(request, 'contracts/data_details.html')
