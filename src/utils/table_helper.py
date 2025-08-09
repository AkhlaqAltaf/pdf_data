import re
from decimal import Decimal

NUMBER_RE = re.compile(r'[-+]?\d[\d,\.]*\d|\d+')

def normalize_number_string(s):
    if s is None:
        return ""
    s = str(s)
    m = NUMBER_RE.search(s.replace(' ', ''))
    if not m:
        return ""
    candidate = m.group(0)
    candidate = candidate.replace(',', '')
    if candidate.count('.') > 1:
        parts = candidate.split('.')
        candidate = ''.join(parts[:-1]) + '.' + parts[-1]
    return candidate.strip()

def safe_decimal_from_raw(raw):
    if raw is None:
        return Decimal('0')
    if isinstance(raw, Decimal):
        return raw
    try:
        normalized = normalize_number_string(raw)
        if not normalized:
            return Decimal('0')
        return Decimal(normalized)
    except Exception:
        try:
            cleaned = re.sub(r'[^0-9.]', '', str(raw))
            return Decimal(cleaned) if cleaned else Decimal('0')
        except Exception:
            return Decimal('0')

def safe_int_from_raw(raw):
    if raw is None:
        return 0
    if isinstance(raw, int):
        return raw
    try:
        s = str(raw)
        m = NUMBER_RE.search(s.replace(' ', ''))
        if not m:
            return 0
        digits = m.group(0).split('.')[0].replace(',', '')
        return int(digits) if digits else 0
    except Exception:
        cleaned = re.sub(r'[^0-9]', '', str(raw))
        return int(cleaned) if cleaned else 0
