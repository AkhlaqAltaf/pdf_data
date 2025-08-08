# contracts/utils.py
import pathlib
from io import BytesIO
from typing import Union, Dict, Any, List
import re

# Try to import the preferred pdf libraries
try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    from PyPDF2 import PdfReader
except Exception:
    PdfReader = None

# Try to import OCR libraries
try:
    import pytesseract
    from PIL import Image
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False

try:
    import cv2
    import numpy as np
    CV_AVAILABLE = True
except ImportError:
    CV_AVAILABLE = False


def _clean_text_enhanced(text: str) -> str:
    """Enhanced text cleaning with better noise removal and formatting preservation."""
    if not text:
        return ""
    
    # Remove common PDF artifacts
    text = re.sub(r'\(cid:\d+\)', '', text)  # Remove CID references
    text = re.sub(r'\xa0', ' ', text)  # Replace non-breaking spaces
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)  # Remove control chars
    
    # Normalize whitespace while preserving structure
    text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces to single
    text = re.sub(r'\r\n?', '\n', text)  # Normalize line endings
    text = re.sub(r'\n{4,}', '\n\n\n', text)  # Limit consecutive newlines
    
    # Clean up common OCR artifacts
    text = re.sub(r'[|]{2,}', '||', text)  # Fix multiple pipes
    text = re.sub(r'[=]{3,}', '===', text)  # Fix multiple equals
    text = re.sub(r'[-]{3,}', '---', text)  # Fix multiple dashes
    
    return text.strip()


def _clean_text(text: str) -> str:
    """Enhanced text cleaning with better noise removal and formatting preservation."""
    if not text:
        return ""
    
    # Remove common PDF artifacts
    text = re.sub(r'\(cid:\d+\)', '', text)  # Remove CID references
    text = re.sub(r'\xa0', ' ', text)  # Replace non-breaking spaces
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)  # Remove control chars
    
    # Normalize whitespace while preserving structure
    text = re.sub(r'[ \t]+', ' ', text)  # Multiple spaces to single
    text = re.sub(r'\r\n?', '\n', text)  # Normalize line endings
    text = re.sub(r'\n{4,}', '\n\n\n', text)  # Limit consecutive newlines
    
    # Clean up common OCR artifacts
    text = re.sub(r'[|]{2,}', '||', text)  # Fix multiple pipes
    text = re.sub(r'[=]{3,}', '===', text)  # Fix multiple equals
    text = re.sub(r'[-]{3,}', '---', text)  # Fix multiple dashes
    
    return text.strip()


def _extract_tables_from_page(page) -> List[Dict[str, Any]]:
    """Extract tables from a PDF page using pdfplumber."""
    tables = []
    try:
        page_tables = page.extract_tables()
        for table in page_tables:
            if table and any(any(cell and str(cell).strip() for cell in row) for row in table):
                # Clean table data
                cleaned_table = []
                for row in table:
                    cleaned_row = []
                    for cell in row:
                        if cell:
                            cleaned_cell = _clean_text(str(cell))
                            if cleaned_cell:
                                cleaned_row.append(cleaned_cell)
                        else:
                            cleaned_row.append("")
                    if any(cell for cell in cleaned_row):  # Only add non-empty rows
                        cleaned_table.append(cleaned_row)
                
                if cleaned_table:
                    tables.append({
                        'type': 'table',
                        'data': cleaned_table,
                        'rows': len(cleaned_table),
                        'cols': max(len(row) for row in cleaned_table) if cleaned_table else 0
                    })
    except Exception as e:
        print(f"Error extracting tables: {e}")
    
    return tables


def _extract_text_with_structure(pdf_stream) -> Dict[str, Any]:
    """
    Extract text with structural information including tables and formatting.
    """
    result = {
        'text': '',
        'tables': [],
        'pages': [],
        'has_tables': False,
        'method': 'unknown',
        'pages_count': 0,
        'ocr_used': False
    }
    
    try:
        if pdfplumber is not None:
            result['method'] = 'pdfplumber'
            with pdfplumber.open(pdf_stream) as doc:
                page_texts = []
                all_tables = []
                
                for page_num, page in enumerate(doc.pages):
                    page_data = {
                        'page_num': page_num + 1,
                        'text': '',
                        'tables': []
                    }
                    
                    # Extract text
                    page_text = page.extract_text()
                    if page_text:
                        page_text = _clean_text(page_text)
                        page_data['text'] = page_text
                        page_texts.append(page_text)
                    
                    # Extract tables
                    page_tables = _extract_tables_from_page(page)
                    page_data['tables'] = page_tables
                    all_tables.extend(page_tables)
                    
                    result['pages'].append(page_data)
                
                result['text'] = '\n\n'.join(page_texts)
                result['tables'] = all_tables
                result['has_tables'] = len(all_tables) > 0
                result['pages_count'] = len(result['pages'])
                
        elif PdfReader is not None:
            result['method'] = 'PyPDF2'
            reader = PdfReader(pdf_stream)
            page_texts = []
            
            for page in reader.pages:
                try:
                    page_text = page.extract_text()
                    if page_text:
                        page_text = _clean_text(page_text)
                        page_texts.append(page_text)
                except Exception as e:
                    print(f"Error extracting text from page: {e}")
                    continue
            
            result['text'] = '\n\n'.join(page_texts)
            result['pages_count'] = len(reader.pages)
            
        else:
            raise ImportError("No PDF parsing library found. Install 'pdfplumber' or 'PyPDF2'.")
            
    except Exception as e:
        print(f"Error in text extraction: {e}")
        result['error'] = str(e)
    
    return result


def read_pdf_text(pdf: Union[str, pathlib.Path, object]) -> str:
    """
    Read a PDF and return a simple concatenated plain-text string.
    (Legacy function for backward compatibility)
    """
    result = read_pdf_with_structure(pdf)
    return result.get('text', '')


def read_pdf_with_structure(pdf: Union[str, pathlib.Path, object]) -> Dict[str, Any]:
    """
    Read a PDF and return structured data including text, tables, and metadata.
    
    Returns:
        Dict with keys:
        - text: concatenated text string
        - tables: list of extracted tables
        - pages: list of page data
        - has_tables: boolean indicating if tables were found
        - extraction_method: method used ('pdfplumber', 'PyPDF2', or 'unknown')
        - error: error message if extraction failed
    """
    # Prepare a binary stream
    stream = None
    opened_file = None

    # 1) If path-like
    if isinstance(pdf, (str, pathlib.Path)):
        opened_file = open(str(pdf), "rb")
        stream = opened_file
    else:
        # 2) TemporaryUploadedFile (has temporary_file_path())
        if hasattr(pdf, "temporary_file_path"):
            opened_file = open(pdf.temporary_file_path(), "rb")
            stream = opened_file
        # 3) File-like with read()
        elif hasattr(pdf, "read"):
            # If it's a Django uploaded file, reading it will consume the stream.
            # We create a BytesIO buffer so pdf libs can seek freely.
            data = pdf.read()
            if isinstance(data, str):
                # unlikely, but protect
                data = data.encode("utf-8")
            stream = BytesIO(data)
        else:
            raise TypeError("Unsupported pdf input. Provide a file path, Django UploadedFile, or file-like object.")

    try:
        result = _extract_text_with_structure(stream)
        
        # If no text extracted and OCR is available, try OCR
        if not result.get('text') and OCR_AVAILABLE and CV_AVAILABLE:
            result = _try_ocr_extraction(stream, result)
            
        return result
        
    finally:
        # Close any file we explicitly opened
        if opened_file:
            try:
                opened_file.close()
            except Exception:
                pass


def _try_ocr_extraction(stream, previous_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Attempt OCR extraction when regular text extraction fails.
    """
    try:
        # Reset stream position
        stream.seek(0)
        
        # Convert PDF pages to images and extract text using OCR
        if pdfplumber is not None:
            with pdfplumber.open(stream) as doc:
                ocr_texts = []
                for page in doc.pages:
                    # Convert page to image
                    img = page.to_image()
                    if img:
                        # Convert to PIL Image
                        pil_img = Image.fromarray(img.original)
                        
                        # Extract text using OCR
                        ocr_text = pytesseract.image_to_string(pil_img, lang='eng+hin')
                        if ocr_text:
                            ocr_text = _clean_text(ocr_text)
                            ocr_texts.append(ocr_text)
                
                if ocr_texts:
                    previous_result['text'] = '\n\n'.join(ocr_texts)
                    previous_result['method'] = 'ocr'
                    previous_result['ocr_used'] = True
                    
    except Exception as e:
        print(f"OCR extraction failed: {e}")
        previous_result['ocr_error'] = str(e)
    
    return previous_result


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
    
    # Remove ALL Hindi words and characters completely
    english_part = re.sub(r'[\u0900-\u097F\u0980-\u09FF\u0A00-\u0A7F\u0A80-\u0AFF\u0B00-\u0B7F\u0B80-\u0BFF\u0C00-\u0C7F\u0C80-\u0CFF\u0D00-\u0D7F\u0D80-\u0DFF\u0E00-\u0E7F\u0E80-\u0EFF\u0F00-\u0FFF]+', '', english_part)
    
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
            # Remove any remaining Hindi characters
            english_line = re.sub(r'[\u0900-\u097F\u0980-\u09FF\u0A00-\u0A7F\u0A80-\u0AFF\u0B00-\u0B7F\u0B80-\u0BFF\u0C00-\u0C7F\u0C80-\u0CFF\u0D00-\u0D7F\u0D80-\u0DFF\u0E00-\u0E7F\u0E80-\u0EFF\u0F00-\u0FFF]+', '', english_line)
            english_lines.append(english_line.strip())
    
    return '\n'.join(english_lines)
