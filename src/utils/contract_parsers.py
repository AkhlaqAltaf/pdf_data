import re
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from difflib import SequenceMatcher
import json

# -----------------------
# Enhanced Text Processing
# -----------------------

def _clean_text_enhanced(text: str) -> str:
    """Enhanced text cleaning with better preservation of structure."""
    if not text:
        return ""
    
    # Remove common PDF artifacts
    text = re.sub(r'\(cid:\d+\)', '', text)
    text = re.sub(r'\xa0', ' ', text)
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
    
    # Normalize whitespace while preserving structure
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\r\n?', '\n', text)
    text = re.sub(r'\n{4,}', '\n\n\n', text)
    
    # Clean up common artifacts
    text = re.sub(r'[|]{3,}', '||', text)
    text = re.sub(r'[=]{3,}', '===', text)
    text = re.sub(r'[-]{3,}', '---', text)
    
    return text.strip()


def _extract_english_only(text: str) -> str:
    """Extract only English text from bilingual content with better cleaning."""
    if not text:
        return ""
    
    # Split by || separator if present
    if '||' in text:
        parts = text.split('||')
        # Take the last part which is usually English
        english_part = parts[-1].strip()
        # Remove any remaining Hindi characters
        english_part = re.sub(r'[\u0900-\u097F]+', '', english_part)
    else:
        # If no || separator, remove Hindi characters
        english_part = re.sub(r'[\u0900-\u097F]+', '', text)
    
    # Clean up garbled text (repeated characters)
    english_part = re.sub(r'([A-Z])\1+', r'\1', english_part)  # Fix repeated uppercase letters
    english_part = re.sub(r'([a-z])\1+', r'\1', english_part)   # Fix repeated lowercase letters
    
    # Clean up common OCR artifacts
    english_part = re.sub(r'[|]{2,}', '|', english_part)  # Fix multiple pipes
    english_part = re.sub(r'[=]{3,}', '===', english_part)  # Fix multiple equals
    english_part = re.sub(r'[-]{3,}', '---', english_part)  # Fix multiple dashes
    
    # Remove extra whitespace and clean up messy parts
    english_part = re.sub(r'\s+', ' ', english_part)
    english_part = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', english_part)  # Keep only alphanumeric and basic punctuation
    
    return english_part.strip()


def _extract_english_from_pdf(raw_text: str) -> str:
    """Extract and clean English text from entire PDF with better processing."""
    # First clean the text
    cleaned_text = _clean_text_enhanced(raw_text)
    
    # Extract English from each line
    lines = cleaned_text.split('\n')
    english_lines = []
    
    for line in lines:
        english_line = _extract_english_only(line)
        if english_line.strip():
            # Additional cleaning for each line
            english_line = re.sub(r'([A-Z])\1+', r'\1', english_line)  # Fix repeated uppercase
            english_line = re.sub(r'([a-z])\1+', r'\1', english_line)   # Fix repeated lowercase
            english_line = re.sub(r'\s+', ' ', english_line)  # Normalize whitespace
            english_line = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', english_line)  # Keep only clean characters
            english_lines.append(english_line.strip())
    
    return '\n'.join(english_lines)


# -----------------------
# Intelligent Section Detection
# -----------------------

SECTION_HEADERS = {
    'contract': ['contract details', 'contract information', 'contract no'],
    'organisation': ['organisation details', 'organization details', 'buyer organisation', 'type central government'],
    'buyer': ['buyer details', 'purchaser details', 'buyer information'],
    'financial_approval': ['financial approval', 'ifd concurrence', 'administrative approval', 'designation of administrative approval'],
    'paying_authority': ['paying authority', 'payment authority', 'paying details'],
    'seller': ['seller details', 'vendor details', 'supplier details', 'gem seller'],
    'product': ['product details', 'item details', 'goods details', 'product information'],
    'consignee': ['consignee details', 'delivery details', 'delivery address'],
    'specification': ['specification', 'technical specification', 'product specification'],
    'epbg': ['epbg', 'bank guarantee', 'performance bond'],
    'terms': ['terms and conditions', 'terms & conditions', 'general terms']
}

# Enhanced field patterns for better extraction
FIELD_PATTERNS = {
    'contract_no': [
        r'contract\s*no[:\-]?\s*([^\n]+)',
        r'contract\s*number[:\-]?\s*([^\n]+)',
        r'gemc[-\dA-Z]{10,}',
        r'contract\s*id[:\-]?\s*([^\n]+)'
    ],
    'generated_date': [
        r'generated\s*date[:\-]?\s*([^\n]+)',
        r'date[:\-]?\s*(\d{1,2}[-\/]\w{3,9}[-\/]\d{2,4})',
        r'(\d{1,2}[-\/]\w{3,9}[-\/]\d{2,4})'
    ],
    'type': [r'type[:\-]?\s*([^\n]+)'],
    'ministry': [r'ministry[:\-]?\s*([^\n]+)'],
    'department': [r'department[:\-]?\s*([^\n]+)'],
    'organisation_name': [r'organisation\s*name[:\-]?\s*([^\n]+)', r'organization\s*name[:\-]?\s*([^\n]+)'],
    'office_zone': [r'office\s*zone[:\-]?\s*([^\n]+)'],
    'designation': [r'designation[:\-]?\s*([^\n]+)'],
    'contact_no': [r'contact\s*no[:\-]?\s*([^\n]+)', r'phone[:\-]?\s*([^\n]+)'],
    'email': [r'email[:\-]?\s*([^\n]+)', r'email\s*id[:\-]?\s*([^\n]+)'],
    'gstin': [r'gstin[:\-]?\s*([^\n]+)', r'gst[:\-]?\s*([^\n]+)'],
    'address': [r'address[:\-]?\s*([^\n]+)'],
    'ifd_concurrence': [r'ifd\s*concurrence[:\-]?\s*([^\n]+)', r'ifd\s*concurrence[:\-]?\s*no'],
    'admin_approval_designation': [r'designation\s*of\s*administrative\s*approval[:\-]?\s*([^\n]+)', r'administrative\s*approval[:\-]?\s*([^\n]+)'],
    'financial_approval_designation': [r'designation\s*of\s*financial\s*approval[:\-]?\s*([^\n]+)', r'financial\s*approval[:\-]?\s*([^\n]+)'],
    'role': [r'role[:\-]?\s*([^\n]+)'],
    'payment_mode': [r'payment\s*mode[:\-]?\s*([^\n]+)'],
    'gem_seller_id': [r'gem\s*seller\s*id[:\-]?\s*([^\n]+)'],
    'company_name': [r'company\s*name[:\-]?\s*([^\n]+)', r'seller\s*name[:\-]?\s*([^\n]+)'],
    'msme_registration_number': [r'msme\s*registration\s*number[:\-]?\s*([^\n]+)'],
    'product_name': [r'product\s*name[:\-]?\s*([^\n]+)', r'item\s*name[:\-]?\s*([^\n]+)'],
    'brand': [r'brand[:\-]?\s*([^\n]+)'],
    'brand_type': [r'brand\s*type[:\-]?\s*([^\n]+)'],
    'catalogue_status': [r'catalogue\s*status[:\-]?\s*([^\n]+)'],
    'selling_as': [r'selling\s*as[:\-]?\s*([^\n]+)'],
    'category_name_quadrant': [r'category\s*name\s*quadrant[:\-]?\s*([^\n]+)'],
    'model': [r'model[:\-]?\s*([^\n]+)'],
    'hsn_code': [r'hsn\s*code[:\-]?\s*([^\n]+)'],
    'ordered_quantity': [r'ordered\s*quantity[:\-]?\s*(\d+)', r'quantity[:\-]?\s*(\d+)'],
    'unit': [r'unit[:\-]?\s*([^\n]+)'],
    'unit_price': [r'unit\s*price[:\-]?\s*([\d,\.]+)', r'price\s*per\s*unit[:\-]?\s*([\d,\.]+)'],
    'tax_bifurcation': [r'tax\s*bifurcation[:\-]?\s*([\d,\.]+)'],
    'total_price': [r'total\s*price[:\-]?\s*([\d,\.]+)', r'total\s*value[:\-]?\s*([\d,\.]+)'],
    'lot_no': [r'lot\s*no[:\-]?\s*([^\n]+)'],
    'delivery_start': [r'delivery\s*start[:\-]?\s*([^\n]+)'],
    'delivery_end': [r'delivery\s*end[:\-]?\s*([^\n]+)'],
    'delivery_to': [r'delivery\s*to[:\-]?\s*([^\n]+)']
}


def _detect_sections_intelligently(text: str) -> List[Dict[str, Any]]:
    """Intelligently detect sections with their content blocks."""
    lines = text.split('\n')
    sections = []
    current_section = None
    current_content = []
    
    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        
        # Check if this line is a section header
        detected_section = None
        for section_name, headers in SECTION_HEADERS.items():
            for header in headers:
                if header in line_lower:
                    detected_section = section_name
                    break
            if detected_section:
                break
        
        # If we found a new section header
        if detected_section:
            # Save previous section if exists
            if current_section:
                sections.append({
                    'name': current_section,
                    'header': current_section,
                    'content': '\n'.join(current_content),
                    'start_line': i - len(current_content),
                    'end_line': i
                })
            
            # Start new section
            current_section = detected_section
            current_content = [line]
        else:
            # Add to current section content only if we have a current section
            if current_section:
                # Check if this line might be the start of a new section (even if not detected)
                # This helps prevent mixing of sections
                potential_new_section = False
                for section_name, headers in SECTION_HEADERS.items():
                    for header in headers:
                        if any(keyword in line_lower for keyword in ['details', 'information', 'authority', 'approval', 'seller', 'buyer', 'product', 'specification']):
                            if section_name != current_section:
                                potential_new_section = True
                                break
                    if potential_new_section:
                        break
                
                if not potential_new_section:
                    current_content.append(line)
                else:
                    # This might be a new section, so end current one and start new
                    sections.append({
                        'name': current_section,
                        'header': current_section,
                        'content': '\n'.join(current_content),
                        'start_line': i - len(current_content),
                        'end_line': i
                    })
                    current_section = None
                    current_content = []
    
    # Add the last section
    if current_section:
        sections.append({
            'name': current_section,
            'header': current_section,
            'content': '\n'.join(current_content),
            'start_line': len(lines) - len(current_content),
            'end_line': len(lines)
        })
    
    return sections


def _extract_field_value(text: str, field_name: str) -> Optional[str]:
    """Extract field value using multiple patterns with better cleaning."""
    if field_name not in FIELD_PATTERNS:
        return None
    
    for pattern in FIELD_PATTERNS[field_name]:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value:
                # Clean the extracted value
                value = re.sub(r'[|]{2,}', '|', value)  # Fix multiple pipes
                value = re.sub(r'[=]{3,}', '===', value)  # Fix multiple equals
                value = re.sub(r'[-]{3,}', '---', value)  # Fix multiple dashes
                value = re.sub(r'\s+', ' ', value)  # Normalize whitespace
                value = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', value)  # Keep only clean characters
                value = value.strip()
                # Only return if value is meaningful (not just punctuation or too short)
                if value and value != ':' and value != '|' and len(value) > 1 and not value.startswith('::'):
                    return value
    
    return None


def _parse_table_intelligently(table_data: List[List[str]]) -> Dict[str, Any]:
    """Intelligently parse table data with better text cleaning."""
    if not table_data or len(table_data) < 2:
        return {}
    
    headers = table_data[0]
    rows = table_data[1:]
    
    # Clean headers
    cleaned_headers = []
    for header in headers:
        english_header = _extract_english_only(header)
        # Additional cleaning for headers
        english_header = re.sub(r'([A-Z])\1+', r'\1', english_header)
        english_header = re.sub(r'([a-z])\1+', r'\1', english_header)
        english_header = re.sub(r'[|]{2,}', '|', english_header)
        english_header = re.sub(r'\s+', ' ', english_header)
        cleaned_headers.append(english_header.strip())
    
    # Identify column types
    column_types = {}
    for i, header in enumerate(cleaned_headers):
        header_lower = header.lower()
        if any(word in header_lower for word in ['product', 'item', 'name', 'description']):
            column_types[i] = 'product_name'
        elif any(word in header_lower for word in ['quantity', 'qty', 'ordered']):
            column_types[i] = 'quantity'
        elif any(word in header_lower for word in ['price', 'cost', 'amount', 'unit']):
            column_types[i] = 'price'
        elif any(word in header_lower for word in ['brand', 'make']):
            column_types[i] = 'brand'
        elif any(word in header_lower for word in ['model', 'type']):
            column_types[i] = 'model'
        elif any(word in header_lower for word in ['specification', 'spec']):
            column_types[i] = 'specification'
        elif any(word in header_lower for word in ['lot', 'no']):
            column_types[i] = 'lot_no'
        elif any(word in header_lower for word in ['delivery', 'date']):
            column_types[i] = 'delivery_date'
    
    # Extract data
    extracted_data = {}
    products = []
    specifications = []
    
    for row in rows:
        if len(row) >= len(cleaned_headers):
            row_data = {}
            for i, cell in enumerate(row):
                if i < len(cleaned_headers) and cell.strip():
                    english_cell = _extract_english_only(cell)
                    if english_cell:
                        # Additional cleaning for cell content
                        english_cell = re.sub(r'([A-Z])\1+', r'\1', english_cell)
                        english_cell = re.sub(r'([a-z])\1+', r'\1', english_cell)
                        english_cell = re.sub(r'[|]{2,}', '|', english_cell)
                        english_cell = re.sub(r'\s+', ' ', english_cell)
                        english_cell = english_cell.strip()
                        
                        if english_cell and english_cell != ':' and english_cell != '|':
                            if i in column_types:
                                row_data[column_types[i]] = english_cell
                            else:
                                row_data[f'col_{i}'] = english_cell
            
            if row_data:
                if 'product_name' in row_data:
                    products.append(row_data)
                elif 'specification' in row_data:
                    specifications.append(row_data)
                else:
                    extracted_data.update(row_data)
    
    if products:
        extracted_data['products'] = products
    if specifications:
        extracted_data['specifications'] = specifications
    
    return extracted_data


def _extract_data_from_section(section: Dict[str, Any]) -> Dict[str, Any]:
    """Extract structured data from a section with enhanced cleaning."""
    section_name = section['name']
    content = section['content']
    
    # Clean the content first
    content = _extract_english_only(content)
    content = re.sub(r'([A-Z])\1+', r'\1', content)  # Fix repeated uppercase
    content = re.sub(r'([a-z])\1+', r'\1', content)   # Fix repeated lowercase
    content = re.sub(r'[|]{2,}', '|', content)  # Fix multiple pipes
    content = re.sub(r'\s+', ' ', content)  # Normalize whitespace
    content = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', content)  # Keep only clean characters
    
    data = {}
    
    if section_name == 'contract':
        # Extract contract number and date separately
        contract_match = re.search(r'contract\s*no[:\-]?\s*([^\n]+)', content, re.IGNORECASE)
        if contract_match:
            contract_no = contract_match.group(1).strip()
            contract_no = re.sub(r'[^\w\-\d]', '', contract_no)  # Keep only alphanumeric and hyphens
            if len(contract_no) > 5:
                data['contract_no'] = contract_no
        
        date_match = re.search(r'generated\s*date[:\-]?\s*([^\n]+)', content, re.IGNORECASE)
        if date_match:
            date_value = date_match.group(1).strip()
            date_value = re.sub(r'[^\w\-\d]', '', date_value)
            if len(date_value) > 5:
                data['generated_date'] = date_value
    
    elif section_name == 'organisation':
        # Extract organization fields with better boundaries
        type_match = re.search(r'type[:\-]?\s*([^\n]+?)(?=\s*(?:ministry|department|organisation|contact|email|gstin|address|office|designation))', content, re.IGNORECASE)
        if type_match:
            org_type = type_match.group(1).strip()
            org_type = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', org_type)
            if len(org_type) > 2:
                data['type'] = org_type
        
        ministry_match = re.search(r'ministry[:\-]?\s*([^\n]+?)(?=\s*(?:department|organisation|contact|email|gstin|address|office|designation))', content, re.IGNORECASE)
        if ministry_match:
            ministry = ministry_match.group(1).strip()
            ministry = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', ministry)
            if len(ministry) > 2:
                data['ministry'] = ministry
        
        dept_match = re.search(r'department[:\-]?\s*([^\n]+?)(?=\s*(?:organisation|contact|email|gstin|address|office|designation))', content, re.IGNORECASE)
        if dept_match:
            department = dept_match.group(1).strip()
            department = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', department)
            if len(department) > 2:
                data['department'] = department
        
        org_match = re.search(r'organisation\s*name[:\-]?\s*([^\n]+?)(?=\s*(?:contact|email|gstin|address|office|designation))', content, re.IGNORECASE)
        if org_match:
            org_name = org_match.group(1).strip()
            org_name = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', org_name)
            if len(org_name) > 2:
                data['organisation_name'] = org_name
    
    elif section_name == 'buyer':
        # Extract buyer fields with better boundaries
        designation_match = re.search(r'designation[:\-]?\s*([^\n]+?)(?=\s*(?:contact|email|gstin|address|office|ministry|department))', content, re.IGNORECASE)
        if designation_match:
            designation = designation_match.group(1).strip()
            designation = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', designation)
            if len(designation) > 2:
                data['designation'] = designation
        
        contact_match = re.search(r'contact\s*no[:\-]?\s*([^\n]+?)(?=\s*(?:email|gstin|address|office|ministry|department))', content, re.IGNORECASE)
        if contact_match:
            contact = contact_match.group(1).strip()
            contact = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', contact)
            if len(contact) > 2:
                data['contact_no'] = contact
        
        email_match = re.search(r'email[:\-]?\s*([^\n]+?)(?=\s*(?:gstin|address|office|ministry|department))', content, re.IGNORECASE)
        if email_match:
            email = email_match.group(1).strip()
            email = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', email)
            if len(email) > 2:
                data['email'] = email
        
        gstin_match = re.search(r'gstin[:\-]?\s*([^\n]+?)(?=\s*(?:address|office|ministry|department))', content, re.IGNORECASE)
        if gstin_match:
            gstin = gstin_match.group(1).strip()
            gstin = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', gstin)
            if len(gstin) > 2:
                data['gstin'] = gstin
    
    elif section_name == 'financial_approval':
        # Enhanced financial approval extraction
        ifd_match = re.search(r'ifd\s*concurrence[:\-]?\s*(no|yes)', content, re.IGNORECASE)
        data['ifd_concurrence'] = ifd_match.group(1).lower() if ifd_match else None
        
        admin_match = re.search(r'designation\s*of\s*administrative\s*approval[:\-]?\s*([^\n]+?)(?=\s*(?:payment|designation|email|gstin|address))', content, re.IGNORECASE)
        if admin_match:
            admin_designation = admin_match.group(1).strip()
            admin_designation = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', admin_designation)
            data['admin_approval_designation'] = admin_designation if len(admin_designation) > 2 else None
        else:
            data['admin_approval_designation'] = None
            
        financial_match = re.search(r'designation\s*of\s*financial\s*approval[:\-]?\s*([^\n]+?)(?=\s*(?:payment|email|gstin|address))', content, re.IGNORECASE)
        if financial_match:
            financial_designation = financial_match.group(1).strip()
            financial_designation = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', financial_designation)
            data['financial_approval_designation'] = financial_designation if len(financial_designation) > 2 else None
        else:
            data['financial_approval_designation'] = None
    
    elif section_name == 'paying_authority':
        # Extract paying authority fields with better boundaries
        role_match = re.search(r'role[:\-]?\s*([^\n]+?)(?=\s*(?:designation|payment|email|gstin|address))', content, re.IGNORECASE)
        if role_match:
            role = role_match.group(1).strip()
            role = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', role)
            if len(role) > 2:
                data['role'] = role
        
        payment_match = re.search(r'payment\s*mode[:\-]?\s*([^\n]+?)(?=\s*(?:designation|email|gstin|address))', content, re.IGNORECASE)
        if payment_match:
            payment_mode = payment_match.group(1).strip()
            payment_mode = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', payment_mode)
            if len(payment_mode) > 2:
                data['payment_mode'] = payment_mode
        
        designation_match = re.search(r'designation[:\-]?\s*([^\n]+?)(?=\s*(?:email|gstin|address))', content, re.IGNORECASE)
        if designation_match:
            designation = designation_match.group(1).strip()
            designation = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', designation)
            if len(designation) > 2:
                data['designation'] = designation
    
    elif section_name == 'seller':
        # Extract seller fields with better boundaries
        seller_id_match = re.search(r'gem\s*seller\s*id[:\-]?\s*([^\n]+?)(?=\s*(?:company|contact|email|address|msme|gstin))', content, re.IGNORECASE)
        if seller_id_match:
            seller_id = seller_id_match.group(1).strip()
            seller_id = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', seller_id)
            if len(seller_id) > 2:
                data['gem_seller_id'] = seller_id
        
        company_match = re.search(r'company\s*name[:\-]?\s*([^\n]+?)(?=\s*(?:contact|email|address|msme|gstin))', content, re.IGNORECASE)
        if company_match:
            company_name = company_match.group(1).strip()
            company_name = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', company_name)
            if len(company_name) > 2:
                data['company_name'] = company_name
        
        contact_match = re.search(r'contact\s*no[:\-]?\s*([^\n]+?)(?=\s*(?:email|address|msme|gstin))', content, re.IGNORECASE)
        if contact_match:
            contact = contact_match.group(1).strip()
            contact = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', contact)
            if len(contact) > 2:
                data['contact_no'] = contact
        
        email_match = re.search(r'email[:\-]?\s*([^\n]+?)(?=\s*(?:address|msme|gstin))', content, re.IGNORECASE)
        if email_match:
            email = email_match.group(1).strip()
            email = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', email)
            if len(email) > 2:
                data['email'] = email
    
    elif section_name == 'product':
        # Extract product information from content
        products = _extract_products_from_content(content)
        data['products'] = products
    
    elif section_name == 'specification':
        # Extract specifications from content
        specifications = _extract_specifications_from_content(content)
        data['specifications'] = specifications
    
    elif section_name == 'consignee':
        # Extract consignee information
        consignees = _extract_consignees_from_content(content)
        data['consignees'] = consignees
    
    elif section_name == 'terms':
        # Extract terms and conditions
        terms = _extract_terms_from_content(content)
        data['terms'] = terms
    
    elif section_name == 'epbg':
        data['detail'] = content.strip()
    
    return data


def _extract_products_from_content(content: str) -> List[Dict[str, Any]]:
    """Extract product information from section content with better parsing."""
    products = []
    lines = content.split('\n')
    
    current_product = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Clean the line
        line = re.sub(r'([A-Z])\1+', r'\1', line)  # Fix repeated uppercase
        line = re.sub(r'([a-z])\1+', r'\1', line)   # Fix repeated lowercase
        line = re.sub(r'[|]{2,}', '|', line)  # Fix multiple pipes
        line = re.sub(r'\s+', ' ', line)  # Normalize whitespace
        
        # Look for product name
        product_match = re.search(r'product\s*name[:\-]?\s*([^\n]+)', line, re.IGNORECASE)
        if product_match:
            if current_product:
                products.append(current_product)
            product_name = product_match.group(1).strip()
            # Clean product name
            product_name = re.sub(r'[|]{2,}', '|', product_name)
            product_name = re.sub(r'\s+', ' ', product_name)
            current_product = {'product_name': product_name}
            continue
        
        # Extract other fields
        for field_name in ['brand', 'model', 'hsn_code', 'ordered_quantity', 'unit_price', 'total_price', 'unit']:
            value = _extract_field_value(line, field_name)
            if value and current_product:
                current_product[field_name] = value
    
    if current_product:
        products.append(current_product)
    
    return products


def _extract_specifications_from_content(content: str) -> List[Dict[str, Any]]:
    """Extract specifications from section content."""
    specifications = []
    lines = content.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Look for key-value pairs
        kv_match = re.search(r'([^:]+):\s*([^\n]+)', line)
        if kv_match:
            key = kv_match.group(1).strip()
            value = kv_match.group(2).strip()
            if key and value:
                specifications.append({
                    'category': 'specification',
                    'sub_spec': key,
                    'value': value
                })
    
    return specifications


def _extract_consignees_from_content(content: str) -> List[Dict[str, Any]]:
    """Extract consignee information from section content."""
    consignees = []
    lines = content.split('\n')
    
    current_consignee = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Look for lot number or designation
        lot_match = re.search(r'lot\s*no[:\-]?\s*([^\n]+)', line, re.IGNORECASE)
        if lot_match:
            if current_consignee:
                consignees.append(current_consignee)
            current_consignee = {'lot_no': lot_match.group(1).strip()}
            continue
        
        # Extract other fields
        for field_name in ['designation', 'email', 'contact', 'gstin', 'address', 'quantity', 'delivery_start', 'delivery_end', 'delivery_to']:
            value = _extract_field_value(line, field_name)
            if value and current_consignee:
                current_consignee[field_name] = value
    
    if current_consignee:
        consignees.append(current_consignee)
    
    return consignees


def _extract_terms_from_content(content: str) -> List[str]:
    """Extract terms and conditions from section content."""
    terms = []
    lines = content.split('\n')
    
    for line in lines:
        line = line.strip()
        if line and not line.startswith('Terms') and not line.startswith('Conditions'):
            terms.append(line)
    
    return terms


# -----------------------
# Main Intelligent Parser
# -----------------------

def parse_contract_text_to_json(english_text: str, tables: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Parse English-only contract text into structured JSON format.
    """
    if not english_text:
        return {}
    
    # Initialize result structure
    result = {
        'contract': {},
        'organisation': {},
        'buyer': {},
        'financial_approval': {},
        'paying_authority': {},
        'seller': {},
        'products': [],
        'consignees': [],
        'specifications': [],
        'terms': [],
        'epbg': '',
        'tables': [],
        'sections': [],
        'extraction_metadata': {
            'sections_found': [],
            'tables_processed': 0,
            'extraction_method': 'intelligent_parser_english_only',
            'english_text_length': len(english_text)
        }
    }
    
    # Simple extraction from English text
    _extract_simple_data(english_text, result)
    
    # Process tables if provided
    if tables:
        result['tables'] = _clean_tables_data(tables)
        result['extraction_metadata']['tables_processed'] = len(result['tables'])
    
    return result


def _extract_simple_data(english_text: str, result: Dict[str, Any]):
    """Extract data using simple, reliable patterns."""
    
    # Contract data
    contract_match = re.search(r'contract\s*no[:\-]?\s*([^\n]+)', english_text, re.IGNORECASE)
    if contract_match:
        contract_no = contract_match.group(1).strip()
        contract_no = re.sub(r'[^\w\-\d]', '', contract_no)
        if len(contract_no) > 5:
            result['contract']['contract_no'] = contract_no
    
    date_match = re.search(r'generated\s*date[:\-]?\s*([^\n]+)', english_text, re.IGNORECASE)
    if date_match:
        date_value = date_match.group(1).strip()
        date_value = re.sub(r'[^\w\-\d]', '', date_value)
        if len(date_value) > 5:
            result['contract']['generated_date'] = date_value
    
    # Organization data
    type_match = re.search(r'type[:\-]?\s*([^\n]+?)(?=\s*(?:ministry|department|organisation|contact|email|gstin|address|office|designation))', english_text, re.IGNORECASE)
    if type_match:
        org_type = type_match.group(1).strip()
        org_type = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', org_type)
        if len(org_type) > 2:
            result['organisation']['type'] = org_type
    
    ministry_match = re.search(r'ministry[:\-]?\s*([^\n]+?)(?=\s*(?:department|organisation|contact|email|gstin|address|office|designation))', english_text, re.IGNORECASE)
    if ministry_match:
        ministry = ministry_match.group(1).strip()
        ministry = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', ministry)
        if len(ministry) > 2:
            result['organisation']['ministry'] = ministry
    
    dept_match = re.search(r'department[:\-]?\s*([^\n]+?)(?=\s*(?:organisation|contact|email|gstin|address|office|designation))', english_text, re.IGNORECASE)
    if dept_match:
        department = dept_match.group(1).strip()
        department = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', department)
        if len(department) > 2:
            result['organisation']['department'] = department
    
    org_match = re.search(r'organisation\s*name[:\-]?\s*([^\n]+?)(?=\s*(?:contact|email|gstin|address|office|designation))', english_text, re.IGNORECASE)
    if org_match:
        org_name = org_match.group(1).strip()
        org_name = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', org_name)
        if len(org_name) > 2:
            result['organisation']['organisation_name'] = org_name
    
    # Buyer data
    designation_match = re.search(r'designation[:\-]?\s*([^\n]+?)(?=\s*(?:contact|email|gstin|address|office|ministry|department))', english_text, re.IGNORECASE)
    if designation_match:
        designation = designation_match.group(1).strip()
        designation = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', designation)
        if len(designation) > 2:
            result['buyer']['designation'] = designation
    
    contact_match = re.search(r'contact\s*no[:\-]?\s*([^\n]+?)(?=\s*(?:email|gstin|address|office|ministry|department))', english_text, re.IGNORECASE)
    if contact_match:
        contact = contact_match.group(1).strip()
        contact = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', contact)
        if len(contact) > 2:
            result['buyer']['contact_no'] = contact
    
    email_match = re.search(r'email[:\-]?\s*([^\n]+?)(?=\s*(?:gstin|address|office|ministry|department))', english_text, re.IGNORECASE)
    if email_match:
        email = email_match.group(1).strip()
        email = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', email)
        if len(email) > 2:
            result['buyer']['email'] = email
    
    # Financial approval data
    ifd_match = re.search(r'ifd\s*concurrence[:\-]?\s*(no|yes)', english_text, re.IGNORECASE)
    if ifd_match:
        result['financial_approval']['ifd_concurrence'] = ifd_match.group(1).lower()
    
    admin_match = re.search(r'designation\s*of\s*administrative\s*approval[:\-]?\s*([^\n]+?)(?=\s*(?:payment|designation|email|gstin|address))', english_text, re.IGNORECASE)
    if admin_match:
        admin_designation = admin_match.group(1).strip()
        admin_designation = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', admin_designation)
        if len(admin_designation) > 2:
            result['financial_approval']['admin_approval_designation'] = admin_designation
    
    financial_match = re.search(r'designation\s*of\s*financial\s*approval[:\-]?\s*([^\n]+?)(?=\s*(?:payment|email|gstin|address))', english_text, re.IGNORECASE)
    if financial_match:
        financial_designation = financial_match.group(1).strip()
        financial_designation = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', financial_designation)
        if len(financial_designation) > 2:
            result['financial_approval']['financial_approval_designation'] = financial_designation
    
    # Seller data
    seller_id_match = re.search(r'gem\s*seller\s*id[:\-]?\s*([^\n]+?)(?=\s*(?:company|contact|email|address|msme|gstin))', english_text, re.IGNORECASE)
    if seller_id_match:
        seller_id = seller_id_match.group(1).strip()
        seller_id = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', seller_id)
        if len(seller_id) > 2:
            result['seller']['gem_seller_id'] = seller_id
    
    company_match = re.search(r'company\s*name[:\-]?\s*([^\n]+?)(?=\s*(?:contact|email|address|msme|gstin))', english_text, re.IGNORECASE)
    if company_match:
        company_name = company_match.group(1).strip()
        company_name = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', company_name)
        if len(company_name) > 2:
            result['seller']['company_name'] = company_name
    
    seller_contact_match = re.search(r'contact\s*no[:\-]?\s*([^\n]+?)(?=\s*(?:email|address|msme|gstin))', english_text, re.IGNORECASE)
    if seller_contact_match:
        seller_contact = seller_contact_match.group(1).strip()
        seller_contact = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', seller_contact)
        if len(seller_contact) > 2:
            result['seller']['contact_no'] = seller_contact
    
    seller_email_match = re.search(r'email[:\-]?\s*([^\n]+?)(?=\s*(?:address|msme|gstin))', english_text, re.IGNORECASE)
    if seller_email_match:
        seller_email = seller_email_match.group(1).strip()
        seller_email = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', seller_email)
        if len(seller_email) > 2:
            result['seller']['email'] = seller_email
    
    # Product data - simple extraction
    product_match = re.search(r'product\s*name[:\-]?\s*([^\n]+?)(?=\s*(?:brand|quantity|price|unit))', english_text, re.IGNORECASE)
    if product_match:
        product_name = product_match.group(1).strip()
        product_name = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', product_name)
        if len(product_name) > 2:
            result['products'].append({
                'product_name': product_name
            })
    
    # Extract quantity and price if available
    quantity_match = re.search(r'(\d+)\s*(?:pieces|units|items)', english_text, re.IGNORECASE)
    if quantity_match and result['products']:
        result['products'][0]['ordered_quantity'] = int(quantity_match.group(1))
    
    price_match = re.search(r'(\d+)\s*(?:inr|rs)', english_text, re.IGNORECASE)
    if price_match and result['products']:
        result['products'][0]['unit_price'] = int(price_match.group(1))
    
    # EPBG data
    epbg_match = re.search(r'epbg\s*detail[:\-]?\s*([^\n]+)', english_text, re.IGNORECASE)
    if epbg_match:
        epbg_detail = epbg_match.group(1).strip()
        epbg_detail = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', epbg_detail)
        if len(epbg_detail) > 2:
            result['epbg'] = epbg_detail


def _clean_tables_data(tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Clean table data to remove Hindi words and improve structure."""
    cleaned_tables = []
    
    for table in tables:
        if table.get('type') == 'table' and 'data' in table:
            cleaned_data = []
            for row in table['data']:
                cleaned_row = []
                for cell in row:
                    if cell:
                        # Clean the cell content
                        cleaned_cell = _extract_english_only(str(cell))
                        cleaned_cell = re.sub(r'[^\w\s\-\.\,\:\/\(\)]', '', cleaned_cell)
                        cleaned_cell = re.sub(r'\s+', ' ', cleaned_cell).strip()
                        cleaned_row.append(cleaned_cell if cleaned_cell else "")
                    else:
                        cleaned_row.append("")
                if any(cell for cell in cleaned_row):  # Only add non-empty rows
                    cleaned_data.append(cleaned_row)
            
            if cleaned_data:
                cleaned_tables.append({
                    'type': 'table',
                    'data': cleaned_data,
                    'rows': len(cleaned_data),
                    'cols': max(len(row) for row in cleaned_data) if cleaned_data else 0
                })
    
    return cleaned_tables


def _clean_extracted_data(result: Dict[str, Any]) -> Dict[str, Any]:
    """Clean all extracted data to remove Hindi words and improve quality."""
    
    # Clean contract data
    if result.get('contract'):
        for key, value in result['contract'].items():
            if isinstance(value, str):
                result['contract'][key] = _extract_english_only(value)
    
    # Clean organisation data
    if result.get('organisation'):
        for key, value in result['organisation'].items():
            if isinstance(value, str):
                result['organisation'][key] = _extract_english_only(value)
    
    # Clean buyer data
    if result.get('buyer'):
        for key, value in result['buyer'].items():
            if isinstance(value, str):
                result['buyer'][key] = _extract_english_only(value)
    
    # Clean financial approval data
    if result.get('financial_approval'):
        for key, value in result['financial_approval'].items():
            if isinstance(value, str):
                result['financial_approval'][key] = _extract_english_only(value)
    
    # Clean paying authority data
    if result.get('paying_authority'):
        for key, value in result['paying_authority'].items():
            if isinstance(value, str):
                result['paying_authority'][key] = _extract_english_only(value)
    
    # Clean seller data
    if result.get('seller'):
        for key, value in result['seller'].items():
            if isinstance(value, str):
                result['seller'][key] = _extract_english_only(value)
    
    # Clean products data
    if result.get('products'):
        for product in result['products']:
            if isinstance(product, dict):
                for key, value in product.items():
                    if isinstance(value, str):
                        product[key] = _extract_english_only(value)
    
    # Clean specifications data
    if result.get('specifications'):
        for spec in result['specifications']:
            if isinstance(spec, dict):
                for key, value in spec.items():
                    if isinstance(value, str):
                        spec[key] = _extract_english_only(value)
    
    # Clean terms data
    if result.get('terms'):
        cleaned_terms = []
        for term in result['terms']:
            if isinstance(term, str):
                cleaned_term = _extract_english_only(term)
                if cleaned_term:
                    cleaned_terms.append(cleaned_term)
        result['terms'] = cleaned_terms
    
    # Clean epbg data
    if result.get('epbg') and isinstance(result['epbg'], str):
        result['epbg'] = _extract_english_only(result['epbg'])
    
    return result


# Legacy function for backward compatibility
def parse_contract_text_to_json_legacy(raw_text: str) -> Dict[str, Any]:
    """Legacy parser function for backward compatibility."""
    return parse_contract_text_to_json(raw_text)