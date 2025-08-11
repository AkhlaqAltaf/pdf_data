import fitz  # PyMuPDF
import pandas as pd
import json
import re
import os
import sys
import django
from datetime import datetime
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Setup Django environment
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_data.settings')
django.setup()

from django.core.files import File
from django.core.files.base import ContentFile
from django.utils import timezone
from src.apps.bid_record.models import BidDocument

class GeMBiddingPDFExtractor:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.extracted_data = {}
        self.bid_instance = None
        
    def extract_text_from_pdf(self):
        """Extract text from PDF using PyMuPDF"""
        try:
            doc = fitz.open(self.pdf_path)
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()
            return text
        except Exception as e:
            print(f"Error extracting text from PDF: {e}")
            return ""
    
    def clean_text(self, text):
        """Clean and normalize extracted text"""
        if not text:
            return ""
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('|', ' ')
        # Remove any non-printable characters
        text = ''.join(char for char in text if char.isprintable() or char.isspace())
        return text.strip()
    
    def clean_text_remove_hindi(self, text):
        """Clean text and remove Hindi characters for storage in models"""
        if not text:
            return ""
        
        # Remove Hindi and non-ASCII characters
        text = re.sub(r'[^\x00-\x7F]+', '', text)
        
        # Remove specific mixed text patterns
        patterns_to_remove = [
            r'वdीय.*?ववरण',
            r'वेता.*?ववरण', 
            r'एमएसएमई.*?GSTIN',
            r'जीएसटXआईएन.*?GSTIN',
            r'GST.*?invoice.*?Buyer',
            r'Delivery.*?Instructions.*?NA',
            r'उ पाद.*?ववरण',
            r'MSME Registration number.*?GSTIN',
            r'Registration number.*?GSTIN',
            r'GSTIN.*?R',
            r'Tax invoice.*?Buyer',
            r'Delivery Instructions.*?NA',
            r'उ पाद.*?ववरण',
            r'oMSME Registration number.*?',
            r'MSME Registration number.*?',
            r'Registration number.*?'
        ]
        
        for pattern in patterns_to_remove:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Clean up extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def extract_field_value(self, text, field_pattern, section_text=""):
        """Extract field value using regex pattern"""
        if section_text:
            search_text = section_text
        else:
            search_text = text
            
        match = re.search(field_pattern, search_text, re.IGNORECASE | re.DOTALL)
        if match:
            value = match.group(1).strip()
            return self.clean_text(value)
        return ""
    
    def extract_section_text(self, text, start_marker, end_marker):
        """Extract text between two markers with better boundary handling"""
        start_pos = text.find(start_marker)
        if start_pos == -1:
            return ""
        
        end_pos = text.find(end_marker, start_pos)
        if end_pos == -1:
            # If no end marker, take until the next section
            section_text = text[start_pos:]
        else:
            section_text = text[start_pos:end_pos]
        
        return section_text
    
    def extract_bidding_data(self, text):
        """Extract all bidding data from PDF text"""
        bidding_data = {}
        
        # Core fields - Only extract fields that are actually found in the PDF
        # Extract dated
        date_match = re.search(r'dated\s*:\s*([^\n]+)', text, re.IGNORECASE)
        if date_match:
            date_str = date_match.group(1).strip()
            # Try to parse different date formats
            try:
                for fmt in ['%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%B %d, %Y']:
                    try:
                        if fmt == '%B %d, %Y':
                            # Handle "January 15, 2025" format
                            date_str = date_str.replace(',', '')
                        parsed_date = datetime.strptime(date_str, fmt).date()
                        bidding_data['dated'] = parsed_date
                        break
                    except ValueError:
                        continue
            except:
                bidding_data['dated'] = None
        else:
            # Try alternative date patterns
            date_match = re.search(r'\d{1,2}-[A-Za-z]{3}-\d{4}', text)
            if date_match:
                try:
                    parsed_date = datetime.strptime(date_match.group(0), '%d-%b-%Y').date()
                    bidding_data['dated'] = parsed_date
                except:
                    bidding_data['dated'] = None
            else:
                bidding_data['dated'] = None
        
        # Extract bid number
        bid_match = re.search(r'bid\s*number\s*:\s*([^\n]+)', text, re.IGNORECASE)
        if bid_match:
            bidding_data['bid_number'] = bid_match.group(1).strip()
        else:
            # Try alternative patterns
            bid_match = re.search(r'bid\s*no\s*:\s*([^\n]+)', text, re.IGNORECASE)
            if bid_match:
                bidding_data['bid_number'] = bid_match.group(1).strip()
            else:
                # Try GEM format like GEM2025B6442399
                bidding_match = re.search(r'GEM\d{4}[A-Z]\d+', text)
                if bidding_match:
                    bidding_data['bid_number'] = bidding_match.group(0)
                else:
                    bidding_match = re.search(r'GEM\w*-\d+', text)
                    if bidding_match:
                        bidding_data['bid_number'] = bidding_match.group(0)
                    else:
                        bidding_data['bid_number'] = ""
        
        # Extract beneficiary
        beneficiary_match = re.search(r'beneficiary\s*:\s*([^\n]+)', text, re.IGNORECASE)
        if beneficiary_match:
            bidding_data['beneficiary'] = beneficiary_match.group(1).strip()
        else:
            bidding_data['beneficiary'] = ""
        
        # Extract ministry
        ministry_match = re.search(r'Ministry/State Name\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if ministry_match:
            bidding_data['ministry'] = ministry_match.group(1).strip()
        else:
            # Try alternative patterns
            ministry_match = re.search(r'ministry\s*:\s*([^\n]+)', text, re.IGNORECASE)
            if ministry_match:
                bidding_data['ministry'] = ministry_match.group(1).strip()
            else:
                bidding_data['ministry'] = ""
        
        # Extract department
        dept_match = re.search(r'Department Name\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if dept_match:
            bidding_data['department'] = dept_match.group(1).strip()
        else:
            # Try alternative patterns
            dept_match = re.search(r'department\s*:\s*([^\n]+)', text, re.IGNORECASE)
            if dept_match:
                bidding_data['department'] = dept_match.group(1).strip()
            else:
                bidding_data['department'] = ""
        
        # Extract organisation
        org_match = re.search(r'Organisation Name\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if org_match:
            bidding_data['organisation'] = org_match.group(1).strip()
        else:
            # Try alternative patterns
            org_match = re.search(r'organisation\s*:\s*([^\n]+)', text, re.IGNORECASE)
            if org_match:
                bidding_data['organisation'] = org_match.group(1).strip()
            else:
                bidding_data['organisation'] = ""
        
        # Extract contract period
        period_match = re.search(r'Contract Period\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if period_match:
            bidding_data['contract_period'] = period_match.group(1).strip()
        else:
            # Try alternative patterns
            period_match = re.search(r'contract\s*period\s*:\s*([^\n]+)', text, re.IGNORECASE)
            if period_match:
                bidding_data['contract_period'] = period_match.group(1).strip()
            else:
                bidding_data['contract_period'] = ""
        
        # Extract item category - capture multiple lines but stop at next section
        category_match = re.search(r'Item Category\s*\n([^\n]+(?:\n[^\n]+)*?)(?=\n\w+[^\n]*:|$)', text, re.IGNORECASE | re.DOTALL)
        if category_match:
            # Clean up the captured text to remove Hindi and extra content
            category_text = category_match.group(1).strip()
            # Remove Hindi text and clean up
            category_text = re.sub(r'[^\x00-\x7F]+', '', category_text)  # Remove non-ASCII
            category_text = re.sub(r'\s+', ' ', category_text).strip()  # Normalize whitespace
            bidding_data['item_category'] = category_text
        else:
            # Try alternative patterns
            category_match = re.search(r'item\s*category\s*:\s*([^\n]+(?:\n[^\n]+)*?)(?=\n\w+[^\n]*:|$)', text, re.IGNORECASE | re.DOTALL)
            if category_match:
                category_text = category_match.group(1).strip()
                category_text = re.sub(r'[^\x00-\x7F]+', '', category_text)
                category_text = re.sub(r'\s+', ' ', category_text).strip()
                bidding_data['item_category'] = category_text
            else:
                bidding_data['item_category'] = ""
        
        # BID DETAILS SECTION - Only extract fields that are actually found
        # Extract bid end datetime
        end_match = re.search(r'Bid End Date/Time\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if end_match:
            bidding_data['bid_end_datetime'] = end_match.group(1).strip()
        else:
            # Try alternative patterns
            end_match = re.search(r'bid\s*end\s*date\s*/\s*time\s*:\s*([^\n]+)', text, re.IGNORECASE | re.DOTALL)
            if end_match:
                bidding_data['bid_end_datetime'] = end_match.group(1).strip()
            else:
                bidding_data['bid_end_datetime'] = ""
        
        # Extract bid open datetime
        open_match = re.search(r'Bid Opening\nDate/Time\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if open_match:
            bidding_data['bid_open_datetime'] = open_match.group(1).strip()
        else:
            # Try alternative patterns
            open_match = re.search(r'bid\s*opening\s*date\s*/\s*time\s*:\s*([^\n]+)', text, re.IGNORECASE | re.DOTALL)
            if open_match:
                bidding_data['bid_open_datetime'] = open_match.group(1).strip()
            else:
                bidding_data['bid_open_datetime'] = ""
        
        # Extract bid offer validity days
        validity_match = re.search(r'Bid Offer\nValidity \(From End Date\)\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if validity_match:
            validity_str = validity_match.group(1).strip()
            # Try to extract numeric value
            numeric_match = re.search(r'(\d+)', validity_str)
            if numeric_match:
                try:
                    bidding_data['bid_offer_validity_days'] = int(numeric_match.group(1))
                except:
                    bidding_data['bid_offer_validity_days'] = None
            else:
                bidding_data['bid_offer_validity_days'] = None
        else:
            # Try alternative patterns
            validity_match = re.search(r'bid\s*offer\s*validity\s*\(from\s*end\s*date\)\s*:\s*([^\n]+)', text, re.IGNORECASE | re.DOTALL)
            if validity_match:
                validity_str = validity_match.group(1).strip()
                numeric_match = re.search(r'(\d+)', validity_str)
                if numeric_match:
                    try:
                        bidding_data['bid_offer_validity_days'] = int(numeric_match.group(1))
                    except:
                        bidding_data['bid_offer_validity_days'] = None
                else:
                    bidding_data['bid_offer_validity_days'] = None
            else:
                bidding_data['bid_offer_validity_days'] = None
        
        # Extract similar category
        similar_match = re.search(r'Similar Category\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if similar_match:
            bidding_data['similar_category'] = similar_match.group(1).strip()
        else:
            # Try alternative patterns
            similar_match = re.search(r'similar\s*category\s*:\s*([^\n]+)', text, re.IGNORECASE)
            if similar_match:
                bidding_data['similar_category'] = similar_match.group(1).strip()
            else:
                bidding_data['similar_category'] = ""
        
        # Extract MSE exemption
        mse_exemption_match = re.search(r'MSE Exemption for Years of\nExperience and Turnover\s*\n([^\n]+)', text, re.IGNORECASE | re.DOTALL)
        if mse_exemption_match:
            bidding_data['mse_exemption'] = mse_exemption_match.group(1).strip()
        else:
            # Try alternative patterns
            mse_exemption_match = re.search(r'mse\s*exemption\s*for\s*years\s*of\s*experience\s*and\s*turnover\s*:\s*([^\n]+)', text, re.IGNORECASE | re.DOTALL)
            if mse_exemption_match:
                bidding_data['mse_exemption'] = mse_exemption_match.group(1).strip()
            else:
                bidding_data['mse_exemption'] = ""
        
        # Add source file and raw text
        bidding_data['source_file'] = os.path.basename(self.pdf_path)
        bidding_data['raw_text'] = text
        
        return bidding_data
    
    def check_bid_exists(self, bid_number):
        """Check if bid already exists in database"""
        if not bid_number:
            return False
        return BidDocument.objects.filter(bid_number=bid_number).exists()
    
    def generate_embedding(self, text):
        """Generate embedding for the given text using sentence-transformers"""
        try:
            from sentence_transformers import SentenceTransformer
            
            # Get the embedder model
            model = SentenceTransformer('all-MiniLM-L6-v2')
            
            # Generate embedding for the cleaned text
            embedding = model.encode([text], normalize_embeddings=True)
            
            # Convert numpy array to list of floats for JSON storage
            embedding_list = embedding[0].tolist()
            
            print(f"✅ Generated embedding with {len(embedding_list)} dimensions")
            return embedding_list
            
        except Exception as e:
            print(f"⚠️  Warning: Error generating embedding: {e}")
            return None
    
    def save_to_django_models(self, text):
        """Save extracted data to Django models"""
        try:
            # Check if bid already exists
            bid_number = self.extracted_data.get('bid_number', '')
            
            if self.check_bid_exists(bid_number):
                print(f"⏭️  Bid {bid_number} already exists, skipping...")
                return False
            
            # Store cleaned text (without Hindi) in raw_text field
            cleaned_text = self.clean_text_remove_hindi(text)
            
            # Generate embedding for the cleaned text
            print("🔍 Generating embedding for bid text...")
            embedding = self.generate_embedding(cleaned_text)
            
            # Save the PDF file
            pdf_filename = os.path.basename(self.pdf_path)
            with open(self.pdf_path, 'rb') as pdf_file:
                from django.core.files import File
                pdf_file_obj = File(pdf_file, name=pdf_filename)
                
                # Create BidDocument instance with updated fields including file
                self.bid_instance = BidDocument.objects.create(
                    file=pdf_file_obj,
                    dated=self.extracted_data.get('dated'),
                    source_file=self.extracted_data.get('source_file', ''),
                    bid_number=bid_number,
                    beneficiary=self.extracted_data.get('beneficiary', ''),
                    ministry=self.extracted_data.get('ministry', ''),
                    department=self.extracted_data.get('department', ''),
                    organisation=self.extracted_data.get('organisation', ''),
                    contract_period=self.extracted_data.get('contract_period', ''),
                    item_category=self.extracted_data.get('item_category', ''),
                    bid_end_datetime=self.extracted_data.get('bid_end_datetime', ''),
                    bid_open_datetime=self.extracted_data.get('bid_open_datetime', ''),
                    bid_offer_validity_days=self.extracted_data.get('bid_offer_validity_days'),
                    similar_category=self.extracted_data.get('similar_category', ''),
                    mse_exemption=self.extracted_data.get('mse_exemption', ''),
                    raw_text=cleaned_text,
                    embedding=embedding
                )
            
            print(f"✅ Successfully saved data to Django models for bid: {bid_number}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving to Django models: {e}")
            return False
    
    def extract_all_data(self):
        """Extract all required data from PDF"""
        print(f"📄 Extracting text from PDF: {os.path.basename(self.pdf_path)}")
        text = self.extract_text_from_pdf()
        
        if not text:
            print("❌ No text extracted from PDF")
            return None
        
        print("🔍 Parsing extracted data...")
        
        # Extract bidding data
        self.extracted_data = self.extract_bidding_data(text)
        
        return self.extracted_data
    
    def export_to_excel(self, output_path=None):
        """Export extracted data to Excel"""
        if output_path is None:
            # Auto-generate filename using bid number
            bid_number = self.extracted_data.get('bid_number', 'unknown')
            if bid_number and bid_number != 'unknown':
                # Replace forward slashes with underscores to make valid filename
                safe_filename = bid_number.replace('/', '_').replace('\\', '_')
                output_path = f"extracted_data/{safe_filename}.xlsx"
            else:
                output_path = "extracted_data/gem_bidding_data.xlsx"
        
        # Ensure extracted_data directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            # Clean data for Excel export - remove Hindi text and problematic characters
            clean_data = {}
            for field, value in self.extracted_data.items():
                if field == 'raw_text':
                    # Skip raw text in Excel export
                    continue
                if isinstance(value, str):
                    # Clean the value for Excel
                    clean_value = re.sub(r'[^\x00-\x7F]+', '', value)  # Remove non-ASCII
                    clean_value = re.sub(r'[^\w\s,.-]', '', clean_value)  # Remove special chars
                    clean_value = re.sub(r'\s+', ' ', clean_value).strip()  # Normalize whitespace
                    clean_data[field] = clean_value
                else:
                    clean_data[field] = value
            
            # Create a writer object
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Convert to DataFrame
                df = pd.DataFrame(list(clean_data.items()), columns=['Field', 'Value'])
                # Write to Excel
                df.to_excel(writer, sheet_name='Bidding Data', index=False)
                    
            print(f"📊 Data exported to Excel: {output_path}")
            return True
        except Exception as e:
            print(f"❌ Error exporting to Excel: {e}")
            return False
    
    def export_to_json(self, output_path=None):
        """Export extracted data to JSON"""
        if output_path is None:
            # Auto-generate filename using bid number
            bid_number = self.extracted_data.get('bid_number', 'unknown')
            if bid_number and bid_number != 'unknown':
                # Replace forward slashes with underscores to make valid filename
                safe_filename = bid_number.replace('/', '_').replace('\\', '_')
                output_path = f"extracted_data/{safe_filename}.json"
            else:
                output_path = "extracted_data/gem_bidding_data.json"
        
        # Ensure extracted_data directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.extracted_data, f, indent=2, ensure_ascii=False)
            
            print(f"📄 Data exported to JSON: {output_path}")
            return True
        except Exception as e:
            print(f"❌ Error exporting to JSON: {e}")
            return False
    
    def print_extracted_data(self):
        """Print extracted data in a formatted way"""
        print("\n" + "="*80)
        print("GEM BIDDING EXTRACTED DATA FROM PDF")
        print("="*80)
        
        # Only show fields that have actual values (not empty)
        for field, value in self.extracted_data.items():
            if field != 'raw_text' and value:  # Skip raw text and empty fields
                print(f"{field}: {value}")
        
        print("\n" + "="*80)
        print(f"Total fields extracted: {len([k for k, v in self.extracted_data.items() if k != 'raw_text' and v])}")
        print("="*80)

def find_all_pdfs_recursively(data_dir):
    """Find all PDF files recursively in data directory and subdirectories"""
    pdf_files = []
    
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    
    return pdf_files

def process_single_pdf(pdf_path, thread_id):
    """Process a single PDF file (for multi-threading)"""
    try:
        print(f"🔄 [Thread-{thread_id}] Processing: {os.path.basename(pdf_path)}")
        
        extractor = GeMBiddingPDFExtractor(pdf_path)
        
        # Extract data
        data = extractor.extract_all_data()
        
        if data:
            # Save to Django models
            text = extractor.extract_text_from_pdf()
            if extractor.save_to_django_models(text):
                # Export to Excel and JSON
                extractor.export_to_excel()
                extractor.export_to_json()
                
                print(f"✅ [Thread-{thread_id}] Successfully processed: {os.path.basename(pdf_path)}")
                return True, pdf_path
            else:
                print(f"⏭️  [Thread-{thread_id}] Skipped (already exists): {os.path.basename(pdf_path)}")
                return False, pdf_path
        else:
            print(f"❌ [Thread-{thread_id}] Failed to extract data from: {os.path.basename(pdf_path)}")
            return False, pdf_path
            
    except Exception as e:
        print(f"❌ [Thread-{thread_id}] Error processing {os.path.basename(pdf_path)}: {e}")
        return False, pdf_path

def process_all_pdfs_in_data_directory_multi_threaded(max_workers=4):
    """Process all PDFs in data directory using multi-threading"""
    data_dir = Path(__file__).parent / "data"
    extracted_data_dir = Path(__file__).parent / "extracted_data"
    
    # Ensure directories exist
    data_dir.mkdir(exist_ok=True)
    extracted_data_dir.mkdir(exist_ok=True)
    
    # Find all PDF files recursively
    pdf_files = find_all_pdfs_recursively(str(data_dir))
    
    if not pdf_files:
        print("❌ No PDF files found in data directory or subdirectories")
        return
    
    print(f"📁 Found {len(pdf_files)} PDF files in data directory and subdirectories")
    print(f"🚀 Starting multi-threaded processing with {max_workers} workers...")
    print("="*80)
    
    successful_extractions = 0
    failed_extractions = 0
    skipped_extractions = 0
    
    start_time = time.time()
    
    # Process PDFs using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_pdf = {
            executor.submit(process_single_pdf, pdf_path, i % max_workers + 1): pdf_path 
            for i, pdf_path in enumerate(pdf_files)
        }
        
        # Process completed tasks
        for future in as_completed(future_to_pdf):
            pdf_path = future_to_pdf[future]
            try:
                success, _ = future.result()
                if success:
                    successful_extractions += 1
                else:
                    # Check if it was skipped due to duplicate
                    extractor = GeMBiddingPDFExtractor(pdf_path)
                    bid_data = extractor.extract_bidding_data(extractor.extract_text_from_pdf())
                    bid_number = bid_data.get('bid_number', '')
                    if extractor.check_bid_exists(bid_number):
                        skipped_extractions += 1
                    else:
                        failed_extractions += 1
            except Exception as e:
                failed_extractions += 1
                print(f"❌ Exception in thread for {os.path.basename(pdf_path)}: {e}")
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    print("\n" + "="*80)
    print("📊 MULTI-THREADED EXTRACTION SUMMARY")
    print("="*80)
    print(f"✅ Successful extractions: {successful_extractions}")
    print(f"⏭️  Skipped (duplicates): {skipped_extractions}")
    print(f"❌ Failed extractions: {failed_extractions}")
    print(f"📁 Total PDFs found: {len(pdf_files)}")
    print(f"📂 Excel/JSON files saved to: {extracted_data_dir}")
    print(f"⏱️  Total processing time: {processing_time:.2f} seconds")
    print(f"🚀 Average time per PDF: {processing_time/len(pdf_files):.2f} seconds")
    print("="*80)

def process_all_pdfs_in_data_directory():
    """Process all PDFs in data directory (single-threaded for backward compatibility)"""
    data_dir = Path(__file__).parent / "data"
    extracted_data_dir = Path(__file__).parent / "extracted_data"
    
    # Ensure directories exist
    data_dir.mkdir(exist_ok=True)
    extracted_data_dir.mkdir(exist_ok=True)
    
    # Find all PDF files recursively
    pdf_files = find_all_pdfs_recursively(str(data_dir))
    
    if not pdf_files:
        print("❌ No PDF files found in data directory or subdirectories")
        return
    
    print(f"📁 Found {len(pdf_files)} PDF files in data directory and subdirectories")
    print("="*80)
    
    successful_extractions = 0
    failed_extractions = 0
    skipped_extractions = 0
    
    for pdf_file in pdf_files:
        print(f"\n🔄 Processing: {os.path.basename(pdf_file)}")
        print("-" * 50)
        
        try:
            extractor = GeMBiddingPDFExtractor(pdf_file)
            
            # Extract data
            data = extractor.extract_all_data()
            
            if data:
                # Save to Django models
                text = extractor.extract_text_from_pdf()
                if extractor.save_to_django_models(text):
                    successful_extractions += 1
                    
                    # Export to Excel and JSON
                    extractor.export_to_excel()
                    extractor.export_to_json()
                    
                    # Print extracted data
                    extractor.print_extracted_data()
                else:
                    skipped_extractions += 1
                    print(f"⏭️  Skipped (already exists): {os.path.basename(pdf_file)}")
            else:
                failed_extractions += 1
                print(f"❌ Failed to extract data from {os.path.basename(pdf_file)}")
                
        except Exception as e:
            failed_extractions += 1
            print(f"❌ Error processing {os.path.basename(pdf_file)}: {e}")
        
        print("-" * 50)
    
    print("\n" + "="*80)
    print("📊 EXTRACTION SUMMARY")
    print("="*80)
    print(f"✅ Successful extractions: {successful_extractions}")
    print(f"⏭️  Skipped (duplicates): {skipped_extractions}")
    print(f"❌ Failed extractions: {failed_extractions}")
    print(f"📁 Total PDFs found: {len(pdf_files)}")
    print(f"📂 Excel/JSON files saved to: {extracted_data_dir}")
    print("="*80)

def generate_embeddings_for_existing_bids():
    """Generate embeddings for existing bids that don't have them"""
    try:
        from src.apps.bid_record.models import BidDocument
        
        print("🔍 Checking for bids without embeddings...")
        
        # Find bids without embeddings
        bids_without_embeddings = BidDocument.objects.filter(embedding__isnull=True)
        
        print(f"📊 Found {bids_without_embeddings.count()} bids without embeddings")
        
        if bids_without_embeddings.count() == 0:
            print("✅ All bids already have embeddings!")
            return
        
        # Create extractor instance for embedding generation
        extractor = GeMBiddingPDFExtractor("dummy.pdf")
        
        # Generate embeddings for bids
        bid_count = 0
        for bid in bids_without_embeddings:
            try:
                if bid.raw_text:
                    print(f"🔍 Generating embedding for bid: {bid.bid_number}")
                    embedding = extractor.generate_embedding(bid.raw_text)
                    if embedding:
                        bid.embedding = embedding
                        bid.save()
                        bid_count += 1
                        print(f"✅ Saved embedding for bid: {bid.bid_number}")
                    else:
                        print(f"⚠️  Could not generate embedding for bid: {bid.bid_number}")
                else:
                    print(f"⚠️  Bid {bid.bid_number} has no raw_text")
            except Exception as e:
                print(f"❌ Error generating embedding for bid {bid.bid_number}: {e}")
        
        print(f"\n📊 EMBEDDING GENERATION SUMMARY:")
        print(f"✅ Bids processed: {bid_count}")
        print("="*80)
        
    except Exception as e:
        print(f"❌ Error in generate_embeddings_for_existing_bids: {e}")

def show_help():
    """Display help information for the script"""
    print("🧪 GEM BIDDING DATA EXTRACTOR - COMMAND LINE USAGE")
    print("="*60)
    print("Usage:")
    print("  python data_extractor.py [options] [pdf_file]")
    print("")
    print("Options:")
    print("  --help, -h              Show this help message")
    print("  --generate-embeddings, -ge  Generate embeddings for existing bids")
    print("  --multi-thread, -mt     Process PDFs using multi-threading")
    print("  --workers=N, -w=N       Set number of worker threads (default: 4)")
    print("")
    print("Examples:")
    print("  python data_extractor.py                    # Process all PDFs in data/ directory")
    print("  python data_extractor.py document.pdf       # Process single PDF file")
    print("  python data_extractor.py --multi-thread     # Multi-threaded processing")
    print("  python data_extractor.py --multi-thread --workers=8  # 8 worker threads")
    print("  python data_extractor.py --generate-embeddings      # Generate embeddings")
    print("")
    print("Note: Place PDF files in the 'data/' directory for batch processing")
    print("="*60)

def main():
    # Check for help command first
    if "--help" in sys.argv or "-h" in sys.argv:
        show_help()
        return
    
    # Check for special commands first
    if "--generate-embeddings" in sys.argv or "-ge" in sys.argv:
        # Generate embeddings for existing bids
        print("🚀 Generating embeddings for existing bids...")
        generate_embeddings_for_existing_bids()
    elif "--multi-thread" in sys.argv or "-mt" in sys.argv:
        # Get number of workers from command line
        max_workers = 4  # Default
        for arg in sys.argv:
            if arg.startswith("--workers="):
                max_workers = int(arg.split("=")[1])
                break
            elif arg.startswith("-w="):
                max_workers = int(arg.split("=")[1])
                break
        
        print(f"🚀 Starting multi-threaded processing with {max_workers} workers...")
        process_all_pdfs_in_data_directory_multi_threaded(max_workers)
    elif len(sys.argv) > 1:
        # Check if PDF path is provided as command line argument
        pdf_path = sys.argv[1]
        
        if not os.path.exists(pdf_path):
            print(f"❌ PDF file not found: {pdf_path}")
            return
        
        # Process single PDF
        extractor = GeMBiddingPDFExtractor(pdf_path)
        
        # Extract data
        data = extractor.extract_all_data()
        
        if data:
            # Save to Django models
            text = extractor.extract_text_from_pdf()
            if extractor.save_to_django_models(text):
                # Export to Excel and JSON
                extractor.export_to_excel()
                extractor.export_to_json()
                
                # Print extracted data
                extractor.print_extracted_data()
                
                print("\n" + "="*80)
                print("✅ SINGLE PDF EXTRACTION COMPLETE!")
                print("="*80)
            else:
                print("⏭️  PDF already exists in database")
        else:
            print("❌ Failed to extract data from PDF")
    else:
        # Process all PDFs in data directory (single-threaded)
        process_all_pdfs_in_data_directory()

if __name__ == "__main__":
    main()
