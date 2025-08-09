


import re
from datetime import datetime
from decimal import Decimal, InvalidOperation




# ---------------- Helper functions ----------------

def clean_text(s):
    """Normalize string: handle None, strip whitespace, remove common noisy prefixes."""
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    # Remove 'ID :' or similar prefix, leading dots/colons and extra spaces
    s = re.sub(r'^[\.\s:]*ID\s*[:\-]?\s*', '', s, flags=re.I)
    s = re.sub(r'^[\.\s:]+', '', s)
    # collapse repeated whitespace
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip()


def safe_str(v):
    """Return cleaned string or empty string (never None)."""
    return clean_text(v)


EMAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
PHONE_RE = re.compile(r'[\+\d\-\(\)\s]{6,30}')
NUMBER_RE = re.compile(r'[-+]?\d[\d,\.]*\d|\d+')


import re

# keep EMAIL_RE defined earlier or redefine:
EMAIL_RE = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')

def extract_email(s):
    """
    Robust email extractor:
      - standard email with '@'
      - try common OCR substitutions ([at], (at), ' at ')
      - fallback to 'ID : token' or 'Email ID : token'
      - final fallback: return a dot-separated token like 'i.898010038itbp.gov.in'
    Returns empty string if nothing sensible found.
    """
    if not s:
        return ""

    s = str(s).strip()

    # 1) try normal email first
    m = EMAIL_RE.search(s)
    if m:
        return m.group(0).strip().strip('.,;:-')

    # 2) try to repair common OCR substitutions and retry
    repaired = s
    repaired = re.sub(r'\[at\]|\(at\)|\s+at\s+|\s+AT\s+', '@', repaired, flags=re.I)
    repaired = repaired.replace('[dot]', '.').replace('(dot)', '.')
    m2 = EMAIL_RE.search(repaired)
    if m2:
        return m2.group(0).strip().strip('.,;:-')

    # 3) look for explicit "ID : token" or "Email ID : token" style
    m3 = re.search(r'(?:ID|Email ID|Email)\s*[:\-]\s*(\S+)', s, flags=re.I)
    if m3:
        token = m3.group(1).strip().strip('.,;:-')
        return token

    # 4) fallback: find dot-separated tokens with at least two dots (e.g. a.b.c)
    m4 = re.search(r'[\w\.-]+\.[\w\.-]+\.\w+', s)
    if m4:
        return m4.group(0).strip().strip('.,;:-')

    # last fallback: return the first non-space token after "ID" if present
    m5 = re.search(r'ID\s*[:\-]?\s*(\S+)', s, flags=re.I)
    if m5:
        return m5.group(1).strip().strip('.,;:-')

    return ""


def extract_phone(s):
    if not s:
        return ""
    s = str(s)
    m = PHONE_RE.search(s)
    return m.group(0).strip() if m else ""


def normalize_number_string(s):
    """Make a number-like string suitable for Decimal parsing (best-effort)."""
    if s is None:
        return ""
    s = str(s)
    # Guess first number-like chunk
    m = NUMBER_RE.search(s.replace(' ', ''))
    if not m:
        return ""
    candidate = m.group(0)
    candidate = candidate.replace(',', '')
    # If multiple dots, keep last as decimal separator
    if candidate.count('.') > 1:
        parts = candidate.split('.')
        candidate = ''.join(parts[:-1]) + '.' + parts[-1]
    return candidate


def parse_decimal(s, default=None):
    s_norm = normalize_number_string(s)
    if not s_norm:
        return default
    try:
        return Decimal(s_norm)
    except (InvalidOperation, ValueError):
        cleaned = re.sub(r'[^0-9.]', '', s_norm)
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
    # remove decimals and commas
    digits = digits.replace(',', '').split('.')[0]
    try:
        return int(digits)
    except Exception:
        return default


def parse_date(s):
    """Try multiple date formats and return a date object or None."""
    if not s:
        return None
    s = clean_text(s)
    formats = ['%d-%b-%Y', '%d-%B-%Y', '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%d %b %Y', '%d %B %Y']
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    m = re.search(r'(\d{1,2}[-/]\w{3,}[-/]\d{4}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}-\d{2}-\d{2})', s)
    if m:
        ss = m.group(0)
        for fmt in formats:
            try:
                return datetime.strptime(ss, fmt).date()
            except Exception:
                continue
    return None


# ---------- Heuristic extractor from parsed 'tables' ----------
def extract_from_tables(tables):
    """
    Heuristic extraction from tables block (parsed_data['tables']).
    Returns dict with possible fields: products, consignees, specifications, epbg, terms, totals.
    """
    results = {
        'products': [],
        'consignees': [],
        'specifications': [],
        'epbg': None,
        'terms': [],
        'totals': {}
    }
    if not tables:
        return results

    for table in tables:
        data = table.get('data') or []
        flat = '\n'.join([' | '.join([str(cell or '') for cell in row]) for row in data]).lower()

        # Product table heuristics: look for headers like 'item description', 'ordered quantity', 'unit price', 'hsn'
        if any(h in flat for h in ['item description', 'product details', 'ordered quantity', 'hsn']):
            for row in data:
                cells = [clean_text(cell) for cell in row]
                if not any(cells):
                    continue
                # pick a cell that looks like product name (alpha)
                product_name = None
                qty = None
                unit_price = None
                total_price = None
                hsn = ""
                for c in cells:
                    if c and re.search(r'[A-Za-z]', c) and not re.search(r'ordered quantity|unit price|hsn|total', c, re.I):
                        product_name = product_name or c
                    if c and re.search(r'\d', c):
                        # heuristics: if contains 'kg' or 'kilogram' treat as quantity, else if contains decimals, treat as price
                        if re.search(r'\b(kg|kilogram|kilogramme|kilograms)\b', c.lower()) or re.match(r'^\d{1,3}(?:[,\.]\d{3})*$', c.replace(' ', '')):
                            maybe_int = parse_int(c)
                            if maybe_int is not None:
                                qty = qty or maybe_int
                        else:
                            if unit_price is None:
                                unit_price = parse_decimal(c)
                # try to detect HSN in row
                for c in cells:
                    if re.search(r'\bhsn\b', c.lower()):
                        # take next token or digits from c
                        hh = re.search(r'hsn[:\s]*([A-Za-z0-9\-]+)', c, flags=re.I)
                        if hh:
                            hsn = hh.group(1)
                if product_name:
                    results['products'].append({
                        'product_name': product_name,
                        'ordered_quantity': qty,
                        'unit_price': unit_price,
                        'total_price': total_price,
                        'hsn': hsn
                    })

        # Consignee heuristics: detect 'consigne' or 'delivery start'
        if 'consigne' in flat or 'delivery start' in flat or 'delivery to be completed by' in flat:
            for row in data:
                cells = [clean_text(cell) for cell in row]
                joined = ' | '.join(cells)
                if re.search(r'\d{1,2}[-/]\w{3,}[-/]\d{4}|\d{4}-\d{2}-\d{2}', joined):
                    # try mapping common columns
                    s_no = parse_int(cells[0]) if len(cells) > 0 else None
                    designation = cells[2] if len(cells) > 2 else ''
                    product_name = cells[3] if len(cells) > 3 else ''
                    qty = parse_int(cells[8]) if len(cells) > 8 else None
                    delivery_start = parse_date(cells[10]) if len(cells) > 10 else None
                    delivery_end = parse_date(cells[11]) if len(cells) > 11 else None
                    results['consignees'].append({
                        's_no': s_no,
                        'designation': designation,
                        'address': designation,
                        'product_name': product_name,
                        'quantity': qty,
                        'delivery_start': delivery_start,
                        'delivery_end': delivery_end
                    })

        # EPBG detection
        if 'epbg' in flat:
            epbg_lines = [line for line in flat.splitlines() if 'epbg' in line]
            if epbg_lines:
                results['epbg'] = '\n'.join(epbg_lines)

        # Specifications detection: look for 'Specification' header and subsequent rows
        if 'specification' in flat and len(data) >= 2:
            # try to pick rows with 2-3 columns: [category, sub_spec, value]
            for row in data[2:]:
                cells = [clean_text(cell) for cell in row]
                if any(cells):
                    cat = cells[0] if len(cells) > 0 else ''
                    sub_spec = cells[1] if len(cells) > 1 else ''
                    val = cells[2] if len(cells) > 2 else ''
                    results['specifications'].append({
                        'category': cat,
                        'sub_spec': sub_spec,
                        'value': val
                    })

        # Terms detection
        if 'terms and conditions' in flat or 'terms and conditions' in (table.get('data') or ''):
            lines = [l for l in flat.splitlines() if l.strip()]
            results['terms'].extend(lines)

        # Totals detection (simple)
        if 'total order value' in flat:
            m = re.search(r'total order value[^\d]*(\d[\d,\.\,]+)', flat, flags=re.I)
            if m:
                results['totals']['total_order_value'] = m.group(1)

    return results

