import re
from datetime import datetime
from decimal import InvalidOperation, Decimal


### ---------- Helper parsers / cleaners ----------

def clean_text(s):
    """Normalize string: handle None, strip whitespace, remove leading 'ID :', leading ':' or '. :' prefixes."""
    if not s:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    # Remove common prefixes like "ID :", ". :", ":", "::", "."
    s = re.sub(r'^[\.\s:]*ID\s*[:\-]?\s*', '', s, flags=re.I)
    s = re.sub(r'^[\.\s:]+', '', s)
    # collapse multiple spaces
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip()


EMAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
PHONE_RE = re.compile(r'[\+\d\-\(\)\s]{6,30}')
NUMBER_RE = re.compile(r'[-+]?\d[\d,\.]*\d|\d+')

def extract_email(s):
    """Return normalized email found in messy text or empty string.

    Handles common variants like 'ID : user (at) domain dot com' and GOV domains
    that sometimes arrive without '@' (e.g., 'user110agov.in' -> 'user110@a.gov.in').
    """
    if not s:
        return ""
    # Clean common prefixes like 'ID :' and stray punctuation
    s = clean_text(str(s))
    if not s:
        return ""

    lowered = s.lower()
    # Normalize obfuscations
    lowered = lowered.replace('(at)', '@').replace('[at]', '@').replace('{at}', '@').replace(' at ', '@')
    lowered = lowered.replace('(dot)', '.').replace('[dot]', '.').replace('{dot}', '.').replace(' dot ', '.')

    # Strict match first
    m = EMAIL_RE.search(lowered)
    if m:
        return m.group(0).strip()

    # Heuristic for gov domains missing '@'
    gov_tails = ['gov.in', 'nic.in']
    for tail in gov_tails:
        idx = lowered.find(tail)
        if idx > 0 and '@' not in lowered:
            start_idx = idx
            # extract optional single-letter subdomain like 'a' in a.gov.in
            subdomain = ''
            if idx - 2 >= 0 and lowered[idx - 1].isalnum() and lowered[idx - 2] != '.':
                subdomain = lowered[idx - 1]
                start_idx = idx - 1
            username = lowered[:start_idx].rstrip('.:@ ')
            domain = (subdomain + '.' if subdomain else '') + tail
            candidate = f"{username}@{domain}"
            m2 = EMAIL_RE.search(candidate)
            if m2:
                return m2.group(0).strip()

    # Generic heuristic: split tail if '@' is missing
    m3 = re.search(r'([A-Za-z0-9\-\.]+)\.([A-Za-z]{2,})$', lowered)
    if m3 and '@' not in lowered:
        tail = m3.group(0)
        username = lowered[: lowered.rfind(tail)].rstrip('.:@ ')
        candidate = f"{username}@{tail}"
        m4 = EMAIL_RE.search(candidate)
        if m4:
            return m4.group(0).strip()

    return ""

def extract_phone(s):
    if not s:
        return ""
    s = str(s)
    m = PHONE_RE.search(s)
    return m.group(0).strip() if m else ""

def normalize_number_string(s):
    """
    Try to find the first 'number-like' chunk and normalize to Decimal-friendly string.
    E.g. '887,799,,881199..9922' -> '887799881199.9922' (best-effort)
    """
    if s is None:
        return ""
    s = str(s)
    # pick first match of number-like sequence
    m = NUMBER_RE.search(s.replace(' ', ''))
    if not m:
        return ""
    candidate = m.group(0)
    # remove repeated commas
    candidate = candidate.replace(',', '')
    # If multiple dots: keep last dot as decimal separator
    if candidate.count('.') > 1:
        parts = candidate.split('.')
        candidate = ''.join(parts[:-1]) + '.' + parts[-1]
    return candidate

def parse_decimal(s, default=None):
    s = normalize_number_string(s)
    if s == "":
        return default
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        # fallback: strip non-digit except dot and try again
        cleaned = re.sub(r'[^0-9.]', '', s)
        try:
            return Decimal(cleaned) if cleaned else default
        except Exception:
            return default

def parse_int(s, default=None):
    if s is None:
        return default
    s = str(s)
    m = NUMBER_RE.search(s.replace(' ', ''))
    if not m:
        return default
    digits = m.group(0)
    # remove commas and dots
    digits = digits.replace(',', '').split('.')[0]
    try:
        return int(digits)
    except Exception:
        return default

def parse_date(s):
    """Try several date formats; return date or None"""
    if not s:
        return None
    s = clean_text(s)
    formats = ['%d-%b-%Y', '%d-%B-%Y', '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%d %b %Y', '%d %B %Y']
    # Many PDFs may give '24-Apr-2025' which matches %d-%b-%Y
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    # fallback: try to extract number pattern and attempt iso
    m = re.search(r'(\d{1,2}[-/]\w{3,}[-/]\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}-\d{2}-\d{2})', s)
    if m:
        ss = m.group(0)
        for fmt in formats:
            try:
                return datetime.strptime(ss, fmt).date()
            except Exception:
                continue
    return None


### ---------- Table-driven heuristic extractors ----------

def extract_from_tables(tables):
    """
    Heuristic extraction from tables block (parsed_data['tables']).
    Returns dict with possible fields: products_list, consignees_list, epbg_text, terms_list, totals
    """
    results = {
        'products': [],    # list of dicts
        'consignees': [],  # list of dicts (may include delivery dates)
        'specifications': [],
        'epbg': None,
        'terms': [],
        'totals': {},  # total_order_value etc
    }
    if not tables:
        return results

    for table in tables:
        data = table.get('data') or []
        # flatten the table text content for easy searching
        flat = '\n'.join([' | '.join([str(cell or '') for cell in row]) for row in data])
        flat_lower = flat.lower()

        # If table has 'Product Details' or header like 'Item Description' -> treat following rows as products
        if any(h in flat_lower for h in ['item description', 'product details', 'product details', 'item', 'hsn code']):
            # Try to find product rows: rows that contain non-empty product name and quantity
            # Heuristic: find rows that have a product name and an ordered quantity cell (digits)
            for row in data:
                # convert all cells to cleaned strings
                cells = [clean_text(c) for c in row]
                joined = ' '.join(cells).strip()
                if not joined:
                    continue
                # detect product name cell (contains letters) and quantity cell (contains digits)
                qty = None
                name = None
                unit_price = None
                total_price = None
                # find quantity-like cell
                for c in cells:
                    if re.search(r'\d{1,3}(?:[,\.]\d{3})*(?:[\,\.]\d+)?', c):
                        # treat as potential quantity or price
                        # if it contains 'kg' or 'kilogram' treat as quantity
                        if re.search(r'\b(kg|kilogram|kg\.)\b', c.lower()) or re.search(r'^\d[0-9,\.]*$', c.replace(' ', '')):
                            # parse integer-like
                            maybe_int = parse_int(c)
                            if maybe_int:
                                qty = maybe_int
                            else:
                                # numeric but not integer: treat as price
                                if not unit_price:
                                    unit_price = parse_decimal(c)
                        else:
                            # price or numeric
                            if not unit_price:
                                unit_price = parse_decimal(c)
                # product name: pick first cell with alpha characters and not a header label
                for c in cells:
                    if c and re.search(r'[A-Za-z]', c):
                        # avoid header-like labels
                        if not re.search(r'item description|ordered quantity|unit price|hsn|total', c.lower()):
                            name = c
                            break
                if name:
                    results['products'].append({
                        'product_name': name,
                        'ordered_quantity': qty,
                        'unit_price': unit_price,
                        'total_price': total_price
                    })

        # Consignee block: detect 'Consigne Detail' header or 'Consignee' text
        if 'consigne' in flat_lower or 'consignee' in flat_lower:
            # try to parse lines where there's a product name and quantity / delivery dates
            # find rows having 'Delivery Start' or date-like columns
            for row in data:
                cells = [clean_text(c) for c in row]
                joined = ' | '.join(cells)
                if re.search(r'\d{1,2}[-/][A-Za-z]{3,}[-/]\d{4}|\d{4}-\d{2}-\d{2}', joined):
                    # try to map: [sno, designation, product, lot no, quantity, delivery_start, delivery_end]
                    # heuristic mapping:
                    try:
                        s_no = parse_int(cells[0])
                        designation = cells[2] if len(cells) > 2 else ''
                        product_name = cells[3] if len(cells) > 3 else ''
                        quantity = parse_int(cells[8]) if len(cells) > 8 else None
                        delivery_start = parse_date(cells[10]) if len(cells) > 10 else None
                        delivery_end = parse_date(cells[11]) if len(cells) > 11 else None
                        results['consignees'].append({
                            's_no': s_no,
                            'designation': designation,
                            'address': designation,  # sometimes address is embedded
                            'quantity': quantity,
                            'delivery_start': delivery_start,
                            'delivery_end': delivery_end,
                            'product_name': product_name
                        })
                    except Exception:
                        continue

        # EPBG detection
        if 'epbg' in flat_lower or 'ePBG' in (table.get('data') or ''):
            # find any line that looks like ePBG detail
            epbg_lines = [line for line in flat.splitlines() if 'epbg' in line.lower() or 'ePBG' in line]
            if epbg_lines:
                results['epbg'] = '\n'.join(epbg_lines)

        # Terms detection
        if 'terms and conditions' in flat_lower or 'terms and conditions' in table.get('data', []):
            # extract lines after header
            lines = [l for l in flat.splitlines() if l.strip()]
            results['terms'].extend(lines)

        # Totals: detect "Total Order Value" etc
        if 'total order value' in flat_lower or 'total order value (in inr)' in flat_lower:
            m = re.search(r'total order value[^\d]*(\d[\d,\.\,]+)', flat, flags=re.I)
            if m:
                results['totals']['total_order_value'] = m.group(1)

    return results
