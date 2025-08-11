import re
import pdfplumber
from datetime import datetime

def clean_text(text):
    text = re.sub(r"\(cid:[0-9]+\)", "", text)  # remove cid artifacts
    text = re.sub(r"[\u0900-\u097F]+", "", text)  # remove Hindi
    text = re.sub(r"[ ]{2,}", " ", text)  # collapse spaces
    return text.strip()

def extract_bid_info_from_pdf(file_path):
    with pdfplumber.open(file_path) as pdf:
        raw_text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
    text = clean_text(raw_text)

    data = {
        # Core
        "dated": None,
        "source_file": file_path,
        "bid_number": None,
        "buyer_email": None,
        "beneficiary": None,
        "delivery_address": None,
        "office_name": None,
        "ministry": None,
        "department": None,
        "organisation": None,
        "estimated_bid_value": None,
        "total_quantity": None,
        "contract_period": None,
        "item_category": None,

        # Extra fields
        "bid_end_datetime": None,
        "bid_open_datetime": None,
        "bid_offer_validity_days": None,
        "primary_product_category": None,
        "technical_clarification_time": None,
        "inspection_required": None,
        "evaluation_method": None,
        "mii_purchase_preference": None,
        "mse_purchase_preference": None,
        "delivery_days": None,
        "scope_of_supply": None,
        "option_clause": None,
        "raw_text":text
    }

    # Core extractions
    if m := re.search(r"Bid Number[:\s]+([A-Z0-9\/]+)", text, re.IGNORECASE):
        data["bid_number"] = m.group(1).strip()

    if m := re.search(r"Dated[:\s]+(\d{2}-\d{2}-\d{4})", text):
        data["dated"] = datetime.strptime(m.group(1), "%d-%m-%Y").date()
    elif m := re.search(r"Dated[:\s]+([A-Za-z]+ \d{1,2}, \d{4})", text):
        data["dated"] = datetime.strptime(m.group(1), "%B %d, %Y").date()

    if m := re.search(r"Ministry[^\n]*\n?([A-Za-z &]+)", text):
        data["ministry"] = m.group(1).strip()

    if m := re.search(r"Department Name\s*([A-Za-z &]+)", text):
        data["department"] = m.group(1).strip()

    if m := re.search(r"Organisation Name\s*([A-Za-z &\(\)]+)", text):
        data["organisation"] = m.group(1).strip()

    if m := re.search(r"Office Name\s*([A-Za-z0-9* &]+)", text):
        data["office_name"] = m.group(1).strip()

    if m := re.search(r"Address\s+([A-Za-z0-9, &]+)", text):
        data["delivery_address"] = m.group(1).strip()

    if m := re.search(r"Total Quantity\s+(\d+)", text):
        data["total_quantity"] = int(m.group(1))

    if m := re.search(r"Contract Period\s*([^\n]+)", text, re.IGNORECASE):
        data["contract_period"] = m.group(1).strip()

    if m := re.search(r"Item Category\s*([\s\S]{1,500})GeMARPTS", text, re.IGNORECASE):
        data["item_category"] = " ".join(m.group(1).split())

    # Extra fields
    if m := re.search(r"Bid End Date/Time\s+([0-9]{2}-[0-9]{2}-[0-9]{4} [0-9:]+)", text):
        data["bid_end_datetime"] = m.group(1)

    if m := re.search(r"Bid Opening Date/Time\s+([0-9]{2}-[0-9]{2}-[0-9]{4} [0-9:]+)", text):
        data["bid_open_datetime"] = m.group(1)

    if m := re.search(r"Bid Offer Validity .*?(\d+) \(Days\)", text):
        data["bid_offer_validity_days"] = int(m.group(1))

    if m := re.search(r"Primary product category\s*(.+)", text):
        data["primary_product_category"] = m.group(1).strip()

    if m := re.search(r"Time allowed for Technical Clarifications .*?(\d+) Days", text):
        data["technical_clarification_time"] = f"{m.group(1)} Days"

    if m := re.search(r"Inspection Required.*?(Yes|No)", text, re.IGNORECASE):
        data["inspection_required"] = m.group(1)

    if m := re.search(r"Evaluation Method\s*([^\n]+)", text):
        data["evaluation_method"] = m.group(1).strip()

    if m := re.search(r"MII Purchase Preference\s*(Yes|No)", text):
        data["mii_purchase_preference"] = m.group(1)

    if m := re.search(r"MSE Purchase Preference\s*(Yes|No)", text):
        data["mse_purchase_preference"] = m.group(1)

    if m := re.search(r"Delivery\s+Days\s+(\d+)", text):
        data["delivery_days"] = int(m.group(1))

    if m := re.search(r"Scope of supply .*?: ([^\n]+)", text):
        data["scope_of_supply"] = m.group(1).strip()

    if m := re.search(r"OPTION CLAUSE: ([\s\S]{1,500}?)\n\d+\.", text):
        data["option_clause"] = " ".join(m.group(1).split())


    return data
