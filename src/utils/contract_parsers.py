import re
from datetime import datetime
from typing import Dict, Any, List, Optional
from difflib import SequenceMatcher

# -----------------------
# Improved Helpers
# -----------------------
CID_RE = re.compile(r'\(cid:\d+\)')  # remove (cid:###) noise


def _clean_noise(text: str) -> str:
    """Remove common OCR noise tokens and normalize whitespace."""
    text = CID_RE.sub('', text)
    text = text.replace('\xa0', ' ')
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\r\n?', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _normalize_label_for_match(s: str) -> str:
    """Prepare string for fuzzy matching - keep only English characters"""
    s = re.sub(r'[^A-Za-z]', '', s or '')  # REMOVE NON-ENGLISH CHARACTERS
    s = s.lower()
    if not s:
        return s
    out = [s[0]]
    for ch in s[1:]:
        if ch == out[-1]:
            continue
        out.append(ch)
    return ''.join(out)


def _similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _try_parse_date(s: str) -> Optional[str]:
    """Parse various date formats to ISO string"""
    if not s:
        return None
    s = s.strip()
    s = re.sub(r'generated date[:\-\s]*', '', s, flags=re.IGNORECASE)
    patterns = [
        "%d-%b-%Y", "%d-%B-%Y", "%d %b %Y", "%d %B %Y",
        "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%b-%y", "%d %b %y"
    ]
    for p in patterns:
        try:
            dt = datetime.strptime(s, p)
            return dt.date().isoformat()
        except Exception:
            continue
    return None


def _parse_number(s: str) -> Optional[float]:
    """Extract numeric values from messy strings"""
    if not s:
        return None
    s = re.sub(r'[^\d\.,\-]', '', s)
    s = s.replace(',,', ',')
    parts = s.split(',')
    if len(parts) > 1 and all(p.isdigit() for p in parts):
        s = ''.join(parts)
    s = s.replace(',', '')
    try:
        return float(s)
    except Exception:
        return None


# -----------------------
# Improved Section Detection
# -----------------------
TOP_HEADERS = {
    'contract': ['contract', 'contractno'],
    'organisation': ['organisationdetails'],
    'buyer': ['buyerdetails'],
    'financial_approval': ['financialapprovaldetail'],
    'paying_authority': ['payingauthoritydetails'],
    'seller': ['sellerdetails'],
    'product': ['productdetails'],
    'consignee': ['consigneedetail'],
    'specification': ['specification'],
    'epbg': ['epbgdetail'],
    'terms': ['termsandconditions']
}

# Field mapping focusing on English labels only
FIELD_KEYWORDS = {
    'contract_no': ['contractno'],
    'generated_date': ['generateddate'],
    'type': ['type'],
    'ministry': ['ministry'],
    'department': ['department'],
    'organisation_name': ['organisationname'],
    'office_zone': ['officezone'],
    'designation': ['designation'],
    'contact_no': ['contactno'],
    'email': ['emailid'],
    'gstin': ['gstin'],
    'address': ['address'],
    'ifd_concurrence': ['ifdconcurrence'],
    'admin_approval_designation': ['designationofadministrativeapproval'],
    'financial_approval_designation': ['designationoffinancialapproval'],
    'role': ['role'],
    'payment_mode': ['paymentmode'],
    'gem_seller_id': ['gemsellerid'],
    'company_name': ['companyname'],
    'msme_registration_number': ['msmeregistrationnumber'],
    'product_name': ['productname'],
    'brand': ['brand'],
    'brand_type': ['brandtype'],
    'catalogue_status': ['cataloguestatus'],
    'selling_as': ['sellingas'],
    'category_name_quadrant': ['categorynamequadrant'],
    'model': ['model'],
    'hsn_code': ['hsncode'],
    'ordered_quantity': ['orderedquantity'],
    'unit': ['unit'],
    'unit_price': ['unitprice'],
    'tax_bifurcation': ['taxbifurcation'],
    'total_price': ['totalprice', 'totalordervalue'],
    'specification': ['specification'],
    'sub_spec': ['subspec'],
    'value': ['value'],
    'lot_no': ['lotno'],
    'delivery_start': ['deliverystart'],
    'delivery_end': ['deliveryend'],
    'delivery_to': ['deliveryto']
}


def _best_field_match(text_label: str, candidates: List[str]) -> Optional[str]:
    """Match English-only labels after normalization"""
    if not text_label:
        return None
    nl = _normalize_label_for_match(text_label)
    best = None
    best_score = 0.0
    for c in candidates:
        cn = _normalize_label_for_match(c)
        score = _similar(nl, cn)
        if score > best_score:
            best_score = score
            best = c
    return best if best_score > 0.75 else None


# -----------------------
# Main Parser - Improved
# -----------------------
def parse_contract_text_to_json(raw_text: str) -> Dict[str, Any]:
    """Improved parser focusing on English data extraction"""
    text = _clean_noise(raw_text)
    lines = [ln.strip() for ln in re.split(r'\n+', text) if ln.strip()]

    # Build section index - focus on English parts only
    section_positions = []
    for i, ln in enumerate(lines):
        # Extract English portions (after || separator)
        if '||' in ln:
            eng_part = ln.split('||')[-1].strip()
        else:
            eng_part = re.sub(r'[\u0900-\u097F]+', '', ln)  # Remove Devanagari chars

        eng_norm = _normalize_label_for_match(eng_part)
        for key, variants in TOP_HEADERS.items():
            for v in variants:
                v_norm = _normalize_label_for_match(v)
                if v_norm and eng_norm and (v_norm in eng_norm or _similar(eng_norm, v_norm) > 0.8):
                    section_positions.append((i, key))
                    break

    # Sort positions and create section blocks
    section_positions.sort(key=lambda x: x[0])
    sections = {}
    if section_positions:
        for idx, (pos, key) in enumerate(section_positions):
            start = pos
            end = section_positions[idx + 1][0] if idx + 1 < len(section_positions) else len(lines)
            block = lines[start:end]
            sections.setdefault(key, []).append(block)
    else:
        sections['body'] = [lines]

    result = {
        'contract': {},
        'buyer': {},
        'financial_approval': {},
        'paying_authority': {},
        'seller': {},
        'products': [],
        'consignees': [],
        'specifications': [],
        'terms': [],
        'epbg': "",
        'raw_text_preview': text[:2000]
    }

    # Extract key-values from English portions
    def extract_kv_from_block(block: List[str]) -> Dict[str, str]:
        out = {}
        for ln in block:
            # Process English part only
            if '||' in ln:
                parts = ln.split('||')
                if len(parts) > 1:
                    eng_part = parts[-1].strip()
                else:
                    eng_part = re.sub(r'[\u0900-\u097F]+', '', ln)
            else:
                eng_part = re.sub(r'[\u0900-\u097F]+', '', ln)

            # Split into key-value pairs
            kv_parts = re.split(r'\s*::\s*|\s*:\s*', eng_part, maxsplit=1)
            if len(kv_parts) == 2:
                key_part, val_part = kv_parts[0].strip(), kv_parts[1].strip()
                key_match = None
                for canonical, variants in FIELD_KEYWORDS.items():
                    if _best_field_match(key_part, variants):
                        key_match = canonical
                        break
                if key_match:
                    out[key_match] = val_part
        return out

    # Process Contract section
    if 'contract' in sections:
        for block in sections['contract']:
            kv = extract_kv_from_block(block)
            if 'contract_no' in kv:
                result['contract']['contract_no'] = kv['contract_no']
            if 'generated_date' in kv:
                result['contract']['generated_date'] = _try_parse_date(kv['generated_date'])

    # Process Organisation section
    if 'organisation' in sections:
        org_kv = {}
        for block in sections['organisation']:
            org_kv.update(extract_kv_from_block(block))
        result['contract'].update({
            'type': org_kv.get('type'),
            'ministry': org_kv.get('ministry'),
            'department': org_kv.get('department'),
            'organisation_name': org_kv.get('organisation_name'),
            'office_zone': org_kv.get('office_zone'),
            'address': org_kv.get('address')
        })

    # Process Buyer section
    if 'buyer' in sections:
        buyer_kv = {}
        for block in sections['buyer']:
            buyer_kv.update(extract_kv_from_block(block))
        result['buyer'] = {
            'designation': buyer_kv.get('designation'),
            'contact_no': buyer_kv.get('contact_no'),
            'email': buyer_kv.get('email'),
            'gstin': buyer_kv.get('gstin'),
            'address': buyer_kv.get('address')
        }

    # Process Seller section
    if 'seller' in sections:
        seller_kv = {}
        for block in sections['seller']:
            seller_kv.update(extract_kv_from_block(block))
        result['seller'] = {
            'gem_seller_id': seller_kv.get('gem_seller_id'),
            'company_name': seller_kv.get('company_name'),
            'contact_no': seller_kv.get('contact_no'),
            'email': seller_kv.get('email'),
            'address': seller_kv.get('address'),
            'msme_registration_number': seller_kv.get('msme_registration_number'),
            'gstin': seller_kv.get('gstin')
        }

    # Process Products section - improved extraction
    if 'product' in sections:
        for block in sections['product']:
            # Extract from structured table-like data
            block_text = "\n".join(block)

            # Product name
            p_name = None
            if 'Product Name' in block_text:
                p_name = re.search(r'Product Name\s*[:\-]?\s*([^\n]+)', block_text, re.IGNORECASE)
                p_name = p_name.group(1).strip() if p_name else None

            # Quantity
            qty = None
            qty_match = re.search(r'Quantity\s*[:\-]?\s*(\d+)', block_text, re.IGNORECASE)
            if qty_match:
                qty = int(qty_match.group(1))

            # Unit price
            unit_price = None
            up_match = re.search(r'Unit Price\s*[:\-]?\s*([\d,\.]+)', block_text, re.IGNORECASE)
            if up_match:
                unit_price = _parse_number(up_match.group(1))

            # Total price
            total_price = None
            tp_match = re.search(r'Total (?:Price|Value)\s*[:\-]?\s*([\d,\.]+)', block_text, re.IGNORECASE)
            if tp_match:
                total_price = _parse_number(tp_match.group(1))

            # Brand/model
            brand = re.search(r'Brand\s*[:\-]?\s*([^\n]+)', block_text, re.IGNORECASE)
            brand = brand.group(1).strip() if brand else None

            model = re.search(r'Model\s*[:\-]?\s*([^\n]+)', block_text, re.IGNORECASE)
            model = model.group(1).strip() if model else None

            if p_name or qty or total_price:
                result['products'].append({
                    'product_name': p_name,
                    'brand': brand,
                    'model': model,
                    'ordered_quantity': qty,
                    'unit_price': unit_price,
                    'total_price': total_price
                })

    # Process Consignees
    if 'consignee' in sections:
        for block in sections['consignee']:
            kv = extract_kv_from_block(block)
            if any(kv.values()):  # Only add if we have data
                result['consignees'].append({
                    'designation': kv.get('designation'),
                    'email': kv.get('email'),
                    'contact': kv.get('contact_no'),
                    'gstin': kv.get('gstin'),
                    'address': kv.get('address'),
                    'lot_no': kv.get('lot_no'),
                    'quantity': kv.get('ordered_quantity'),
                    'delivery_start': _try_parse_date(kv.get('delivery_start', '')),
                    'delivery_end': _try_parse_date(kv.get('delivery_end', '')),
                    'delivery_to': kv.get('delivery_to')
                })

    # Process Specifications
    if 'specification' in sections:
        for block in sections['specification']:
            spec_text = "\n".join(block)
            # Extract specification pairs
            specs = {}
            for ln in block:
                eng_part = ln.split('||')[-1] if '||' in ln else re.sub(r'[\u0900-\u097F]+', '', ln)
                parts = re.split(r'\s*:\s*', eng_part, maxsplit=1)
                if len(parts) == 2:
                    key, val = parts[0].strip(), parts[1].strip()
                    if key and val:
                        specs[_normalize_label_for_match(key)] = val
            if specs:
                result['specifications'].append(specs)

    # Final fallbacks for critical fields
    if not result['contract'].get('contract_no'):
        gem_match = re.search(r'GEMC[\-\dA-Z]{10,}', text)
        if gem_match:
            result['contract']['contract_no'] = gem_match.group(0)

    if not result['contract'].get('generated_date'):
        date_match = re.search(r'(\d{1,2}[-\/]\w{3,9}[-\/]\d{2,4})', text)
        if date_match:
            result['contract']['generated_date'] = _try_parse_date(date_match.group(1))

    return result