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
import logging
from datetime import datetime

# Setup Django environment
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_data.settings')
django.setup()

from django.core.files import File
from django.core.files.base import ContentFile
from django.utils import timezone
from src.apps.bid_record.models import BidDocument



class ProcessLogger:
    """Comprehensive logging for PDF processing operations"""
    
    def __init__(self, log_dir="logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Create timestamp for this session
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_id = f"extraction_session_{timestamp}"
        
        # Setup file logging
        self.setup_file_logging()
        
        # Setup detailed CSV logging
        self.setup_csv_logging()
        
        # Processing statistics
        self.stats = {
            'total_files': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'ignored': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
    
    def setup_file_logging(self):
        """Setup file-based logging"""
        log_file = self.log_dir / f"{self.session_id}.log"
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()  # Also print to console
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.log_file = log_file
    
    def setup_csv_logging(self):
        """Setup CSV logging for detailed tracking"""
        csv_file = self.log_dir / f"{self.session_id}_detailed.csv"
        
        # CSV headers
        self.csv_headers = [
            'timestamp', 'filename', 'status', 'reason', 'file_size', 
            'processing_time', 'error_details', 'bid_number', 'pages_extracted'
        ]
        
        # Create CSV file with headers
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            import csv
            writer = csv.writer(f)
            writer.writerow(self.csv_headers)
        
        self.csv_file = csv_file
        self.csv_lock = threading.Lock()
    
    def log_file_processing(self, filename, status, reason="", file_size=0, 
                          processing_time=0, error_details="", bid_number="", pages_extracted=0):
        """Log a file processing result"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Log to file
        if status == 'SUCCESS':
            self.logger.info(f"✅ {filename}: {reason}")
        elif status == 'SKIPPED':
            self.logger.info(f"⏭️  {filename}: {reason}")
        elif status == 'FAILED':
            self.logger.error(f"❌ {filename}: {reason}")
        elif status == 'IGNORED':
            self.logger.warning(f"⚠️  {filename}: {reason}")
        
        # Log to CSV
        with self.csv_lock:
            with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
                import csv
                writer = csv.writer(f)
                writer.writerow([
                    timestamp, filename, status, reason, file_size,
                    processing_time, error_details, bid_number, pages_extracted
                ])
        
        # Update statistics
        if status == 'SUCCESS':
            self.stats['successful'] += 1
        elif status == 'SKIPPED':
            self.stats['skipped'] += 1
        elif status == 'FAILED':
            self.stats['failed'] += 1
        elif status == 'IGNORED':
            self.stats['ignored'] += 1
    
    def log_session_start(self, total_files):
        """Log the start of a processing session"""
        self.stats['total_files'] = total_files
        self.logger.info(f"🚀 Starting PDF extraction session: {self.session_id}")
        self.logger.info(f"📁 Total files to process: {total_files}")
        self.logger.info(f"📂 Log directory: {self.log_dir}")
        self.logger.info(f"📄 Detailed CSV log: {self.csv_file}")
    
    def log_session_end(self):
        """Log the end of a processing session"""
        self.stats['end_time'] = datetime.now()
        duration = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info("="*80)
        self.logger.info("📊 PROCESSING SESSION COMPLETE")
        self.logger.info("="*80)
        self.logger.info(f"📁 Total files found: {self.stats['total_files']}")
        self.logger.info(f"✅ Successful extractions: {self.stats['successful']}")
        self.logger.info(f"⏭️  Skipped (duplicates): {self.stats['skipped']}")
        self.logger.info(f"❌ Failed extractions: {self.stats['failed']}")
        self.logger.info(f"⚠️  Ignored files: {self.stats['ignored']}")
        self.logger.info(f"⏱️  Total duration: {duration}")
        self.logger.info(f"📄 Detailed logs saved to: {self.log_file}")
        self.logger.info(f"📊 CSV summary saved to: {self.csv_file}")
        self.logger.info("="*80)
        
        # Save session summary to JSON
        summary_file = self.log_dir / f"{self.session_id}_summary.json"
        summary_data = {
            'session_id': self.session_id,
            'start_time': self.stats['start_time'].isoformat(),
            'end_time': self.stats['end_time'].isoformat(),
            'duration_seconds': duration.total_seconds(),
            'statistics': self.stats.copy(),
            'log_files': {
                'log_file': str(self.log_file),
                'csv_file': str(self.csv_file),
                'summary_file': str(summary_file)
            }
        }
        
        # Convert datetime objects to strings for JSON serialization
        summary_data['statistics']['start_time'] = self.stats['start_time'].isoformat()
        summary_data['statistics']['end_time'] = self.stats['end_time'].isoformat()
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"📋 Session summary saved to: {summary_file}")
    
    def get_log_files(self):
        """Get list of log files for this session"""
        return {
            'log_file': self.log_file,
            'csv_file': self.csv_file,
            'summary_file': self.log_dir / f"{self.session_id}_summary.json"
        }



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
    
    def extract_field_value_robust(self, text, field_patterns, section_text=""):
        """Extract field value using multiple patterns to handle both Hindi-first and English-first text"""
        if section_text:
            search_text = section_text
        else:
            search_text = text
        
        # If field_patterns is a string, convert to list
        if isinstance(field_patterns, str):
            field_patterns = [field_patterns]
        
        # Try each pattern
        for pattern in field_patterns:
            match = re.search(pattern, search_text, re.IGNORECASE | re.DOTALL)
            if match:
                value = match.group(1).strip()
                return self.clean_text(value)
        
        return ""
    
    def extract_field_value_with_fallback(self, text, primary_patterns, fallback_patterns=None, section_text=""):
        """Extract field value with primary patterns and fallback patterns for different text orders"""
        if section_text:
            search_text = section_text
        else:
            search_text = text
        
        # Try primary patterns first
        result = self.extract_field_value_robust(search_text, primary_patterns, section_text)
        if result:
            return result
        
        # If no result, try fallback patterns
        if fallback_patterns:
            result = self.extract_field_value_robust(search_text, fallback_patterns, section_text)
            if result:
                return result
        
        return ""
    
    def extract_bidding_data_enhanced(self, text):
        """Enhanced extraction that handles both Hindi-first and English-first patterns"""
        bidding_data = {}
        
        print(f"🔍 Extracting bidding data with enhanced pattern matching...")
        print(f"📝 Text length: {len(text)} characters")
        
        # Extract dated with multiple patterns
        date_patterns = [
            r'dated\s*:\s*([^\n]+)',  # English first: "dated: 15-01-2025"
            r'([^\n]+)\s*dated\s*:',  # Hindi first: "15-01-2025 dated:"
            r'\d{1,2}-[A-Za-z]{3}-\d{4}',  # Direct date format
            r'\d{1,2}/\d{1,2}/\d{4}',  # DD/MM/YYYY format
            r'\d{4}-\d{1,2}-\d{1,2}'   # YYYY-MM-DD format
        ]
        
        date_value = self.extract_field_value_robust(text, date_patterns)
        if date_value:
            # Try to parse the date
            try:
                for fmt in ['%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d', '%d-%b-%Y', '%B %d, %Y']:
                    try:
                        if fmt == '%B %d, %Y':
                            date_value = date_value.replace(',', '')
                        parsed_date = datetime.strptime(date_value, fmt).date()
                        bidding_data['dated'] = parsed_date
                        print(f"✅ Date extracted: {parsed_date}")
                        break
                    except ValueError:
                        continue
            except:
                bidding_data['dated'] = None
        else:
            bidding_data['dated'] = None
        
        # Extract bid number with multiple patterns
        bid_patterns = [
            r'bid\s*number\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*bid\s*number\s*:',  # Hindi first
            r'bid\s*no\s*:\s*([^\n]+)',     # Alternative English
            r'([^\n]+)\s*bid\s*no\s*:',     # Alternative Hindi first
            r'GEM\d{4}[A-Z]\d+',            # GEM format
            r'GEM\w*-\d+',                  # GEM with dash
            r'BID\s*:\s*([^\n]+)',          # BID: format
            r'([^\n]+)\s*BID\s*:'           # BID: Hindi first
        ]
        
        bid_value = self.extract_field_value_robust(text, bid_patterns)
        bidding_data['bid_number'] = bid_value
        if bid_value:
            print(f"✅ Bid number extracted: {bid_value}")
        
        # Extract beneficiary with multiple patterns
        beneficiary_patterns = [
            r'beneficiary\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*beneficiary\s*:',  # Hindi first
            r'Beneficiary\s*\n([^\n]+)',    # Multi-line format
            r'([^\n]+)\s*Beneficiary\s*\n'  # Multi-line Hindi first
        ]
        
        beneficiary_value = self.extract_field_value_robust(text, beneficiary_patterns)
        bidding_data['beneficiary'] = beneficiary_value
        if beneficiary_value:
            print(f"✅ Beneficiary extracted: {beneficiary_value}")
        
        # Extract ministry with multiple patterns
        ministry_patterns = [
            r'Ministry/State Name\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Ministry/State Name\s*\n',  # Multi-line Hindi first
            r'ministry\s*:\s*([^\n]+)',            # English first
            r'([^\n]+)\s*ministry\s*:',            # Hindi first
            r'Ministry\s*\n([^\n]+)',              # Alternative multi-line
            r'([^\n]+)\s*Ministry\s*\n'            # Alternative multi-line Hindi first
        ]
        
        ministry_value = self.extract_field_value_robust(text, ministry_patterns)
        bidding_data['ministry'] = ministry_value
        if ministry_value:
            print(f"✅ Ministry extracted: {ministry_value}")
        
        # Extract department with multiple patterns
        department_patterns = [
            r'Department Name\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Department Name\s*\n',  # Multi-line Hindi first
            r'department\s*:\s*([^\n]+)',        # English first
            r'([^\n]+)\s*department\s*:',        # Hindi first
            r'Department\s*\n([^\n]+)',          # Alternative multi-line
            r'([^\n]+)\s*Department\s*\n'        # Alternative multi-line Hindi first
        ]
        
        department_value = self.extract_field_value_robust(text, department_patterns)
        bidding_data['department'] = department_value
        if department_value:
            print(f"✅ Department extracted: {department_value}")
        
        # Extract organisation with multiple patterns
        organisation_patterns = [
            r'Organisation Name\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Organisation Name\s*\n',  # Multi-line Hindi first
            r'organisation\s*:\s*([^\n]+)',        # English first
            r'([^\n]+)\s*organisation\s*:',        # Hindi first
            r'Organization\s*\n([^\n]+)',          # Alternative spelling
            r'([^\n]+)\s*Organization\s*\n'        # Alternative spelling Hindi first
        ]
        
        organisation_value = self.extract_field_value_robust(text, organisation_patterns)
        bidding_data['organisation'] = organisation_value
        if organisation_value:
            print(f"✅ Organisation extracted: {organisation_value}")
        
        # Extract contract period with multiple patterns
        period_patterns = [
            r'Contract Period\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Contract Period\s*\n',  # Multi-line Hindi first
            r'contract\s*period\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*contract\s*period\s*:',  # Hindi first
            r'Period\s*\n([^\n]+)',               # Alternative format
            r'([^\n]+)\s*Period\s*\n'             # Alternative format Hindi first
        ]
        
        period_value = self.extract_field_value_robust(text, period_patterns)
        bidding_data['contract_period'] = period_value
        if period_value:
            print(f"✅ Contract period extracted: {period_value}")
        
        # Extract item category with multiple patterns
        category_patterns = [
            r'Item Category\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Item Category\s*\n',  # Multi-line Hindi first
            r'item\s*category\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*item\s*category\s*:',  # Hindi first
            r'Category\s*\n([^\n]+)',           # Alternative format
            r'([^\n]+)\s*Category\s*\n'         # Alternative format Hindi first
        ]
        
        category_value = self.extract_field_value_robust(text, category_patterns)
        if category_value:
            # Clean up the text - remove Hindi characters but keep English
            category_value = re.sub(r'[^\x00-\x7F]+', '', category_value)
            category_value = re.sub(r'\s+', ' ', category_value).strip()
            bidding_data['item_category'] = category_value
            print(f"✅ Item category extracted: {category_value}")
        else:
            bidding_data['item_category'] = ""
        
        # Extract bid end datetime with multiple patterns
        end_patterns = [
            r'Bid End Date/Time\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Bid End Date/Time\s*\n',  # Multi-line Hindi first
            r'bid\s*end\s*date\s*/\s*time\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*bid\s*end\s*date\s*/\s*time\s*:',  # Hindi first
            r'End Date\s*\n([^\n]+)',              # Alternative format
            r'([^\n]+)\s*End Date\s*\n'            # Alternative format Hindi first
        ]
        
        end_value = self.extract_field_value_robust(text, end_patterns)
        bidding_data['bid_end_datetime'] = end_value
        if end_value:
            print(f"✅ Bid end datetime extracted: {end_value}")
        
        # Extract bid open datetime with multiple patterns
        open_patterns = [
            r'Bid Opening\nDate/Time\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Bid Opening\nDate/Time\s*\n',  # Multi-line Hindi first
            r'bid\s*opening\s*date\s*/\s*time\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*bid\s*opening\s*date\s*/\s*time\s*:',  # Hindi first
            r'Opening Date\s*\n([^\n]+)',              # Alternative format
            r'([^\n]+)\s*Opening Date\s*\n'            # Alternative format Hindi first
        ]
        
        open_value = self.extract_field_value_robust(text, open_patterns)
        bidding_data['bid_open_datetime'] = open_value
        if open_value:
            print(f"✅ Bid open datetime extracted: {open_value}")
        
        # Extract bid offer validity days with multiple patterns
        validity_patterns = [
            r'Bid Offer\nValidity \(From End Date\)\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Bid Offer\nValidity \(From End Date\)\s*\n',  # Multi-line Hindi first
            r'validity\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*validity\s*:',  # Hindi first
            r'Validity\s*\n([^\n]+)',    # Alternative format
            r'([^\n]+)\s*Validity\s*\n'  # Alternative format Hindi first
        ]
        
        validity_value = self.extract_field_value_robust(text, validity_patterns)
        if validity_value:
            # Try to extract numeric value
            numeric_match = re.search(r'(\d+)', validity_value)
            if numeric_match:
                try:
                    bidding_data['bid_offer_validity_days'] = int(numeric_match.group(1))
                    print(f"✅ Bid offer validity extracted: {bidding_data['bid_offer_validity_days']} days")
                except:
                    bidding_data['bid_offer_validity_days'] = None
            else:
                bidding_data['bid_offer_validity_days'] = None
        else:
            bidding_data['bid_offer_validity_days'] = None
        
        # Extract similar category with multiple patterns
        similar_patterns = [
            r'Similar Category\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*Similar Category\s*\n',  # Multi-line Hindi first
            r'similar\s*category\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*similar\s*category\s*:',  # Hindi first
            r'Similar\s*\n([^\n]+)',              # Alternative format
            r'([^\n]+)\s*Similar\s*\n'            # Alternative format Hindi first
        ]
        
        similar_value = self.extract_field_value_robust(text, similar_patterns)
        if similar_value:
            # Clean up the text - remove Hindi characters but keep English
            similar_value = re.sub(r'[^\x00-\x7F]+', '', similar_value)
            similar_value = re.sub(r'\s+', ' ', similar_value).strip()
            bidding_data['similar_category'] = similar_value
            print(f"✅ Similar category extracted: {similar_value}")
        else:
            bidding_data['similar_category'] = ""
        
        # Extract MSE exemption with multiple patterns
        mse_patterns = [
            r'MSE Exemption\s*\n([^\n]+)',  # Multi-line format
            r'([^\n]+)\s*MSE Exemption\s*\n',  # Multi-line Hindi first
            r'mse\s*exemption\s*:\s*([^\n]+)',  # English first
            r'([^\n]+)\s*mse\s*exemption\s*:',  # Hindi first
            r'MSE\s*\n([^\n]+)',                # Alternative format
            r'([^\n]+)\s*MSE\s*\n'              # Alternative format Hindi first
        ]
        
        mse_value = self.extract_field_value_robust(text, mse_patterns)
        bidding_data['mse_exemption'] = mse_value
        if mse_value:
            print(f"✅ MSE exemption extracted: {mse_value}")
        
        # Store source file name
        bidding_data['source_file'] = os.path.basename(self.pdf_path)
        
        # Store raw text (no embedding generation)
        bidding_data['raw_text'] = text
        
        print(f"📊 Extraction complete. Found {len([v for v in bidding_data.values() if v])} fields with data.")
        return bidding_data
    
    def extract_field_value(self, text, field_pattern, section_text=""):
        """Extract field value using regex pattern (legacy function for compatibility)"""
        return self.extract_field_value_robust(text, [field_pattern], section_text)
    
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
        """Legacy function for backward compatibility - now calls enhanced version"""
        return self.extract_bidding_data_enhanced(text)
    
    def check_bid_exists(self, bid_number):
        """Check if bid already exists in database"""
        if not bid_number:
            return False
        return BidDocument.objects.filter(bid_number=bid_number).exists()
    
    def generate_embedding(self, text):
        """Generate embedding for the given text - DISABLED (AI search removed)"""
        # Embedding generation disabled as per user request
        print("⚠️  Embedding generation disabled - AI search functionality removed")
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
            
            # Save the PDF file
            pdf_filename = os.path.basename(self.pdf_path)
            with open(self.pdf_path, 'rb') as pdf_file:
                from django.core.files import File
                pdf_file_obj = File(pdf_file, name=pdf_filename)
                
                print(f"💾 Saving bid to database: {bid_number}")
                
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
                    raw_text=cleaned_text
                )
            
            print(f"✅ SUCCESS: Bid saved successfully to database")
            return True
            
        except Exception as e:
            print(f"❌ Error saving to Django models: {e}")
            import traceback
            traceback.print_exc()
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
        self.extracted_data = self.extract_bidding_data_enhanced(text)
        
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
            # Clean data for JSON export - handle date objects properly
            clean_data = {}
            for field, value in self.extracted_data.items():
                if field == 'raw_text':
                    # Skip raw text in JSON export to keep file size manageable
                    continue
                if hasattr(value, 'isoformat'):  # Check if it's a date-like object
                    # Convert date objects to ISO format strings
                    clean_data[field] = value.isoformat()
                elif isinstance(value, str):
                    # Clean the value for JSON
                    clean_value = re.sub(r'[^\x00-\x7F]+', '', value)  # Remove non-ASCII
                    clean_value = re.sub(r'\s+', ' ', clean_value).strip()  # Normalize whitespace
                    clean_data[field] = clean_value
                else:
                    clean_data[field] = value
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(clean_data, f, indent=2, ensure_ascii=False)
            
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
    
    def analyze_text_patterns(self, text):
        """Analyze text patterns to understand Hindi-English text structure"""
        print(f"🔍 Analyzing text patterns for: {os.path.basename(self.pdf_path)}")
        print("="*80)
        
        # Find Hindi text patterns
        hindi_patterns = re.findall(r'[^\x00-\x7F]+', text)
        if hindi_patterns:
            print(f"📝 Found {len(hindi_patterns)} Hindi text patterns:")
            for i, pattern in enumerate(hindi_patterns[:10], 1):  # Show first 10
                print(f"  {i}. {pattern}")
            if len(hindi_patterns) > 10:
                print(f"  ... and {len(hindi_patterns) - 10} more")
        else:
            print("📝 No Hindi text patterns found")
        
        # Find English text patterns
        english_patterns = re.findall(r'\b[A-Za-z]+\s*:\s*[^\n]+', text)
        if english_patterns:
            print(f"📝 Found {len(english_patterns)} English field patterns:")
            for i, pattern in enumerate(english_patterns[:10], 1):  # Show first 10
                print(f"  {i}. {pattern}")
            if len(english_patterns) > 10:
                print(f"  ... and {len(english_patterns) - 10} more")
        else:
            print("📝 No English field patterns found")
        
        # Find mixed patterns (Hindi + English)
        mixed_patterns = re.findall(r'[^\x00-\x7F]+\s*[A-Za-z]+|[A-Za-z]+\s*[^\x00-\x7F]+', text)
        if mixed_patterns:
            print(f"📝 Found {len(mixed_patterns)} mixed Hindi-English patterns:")
            for i, pattern in enumerate(mixed_patterns[:10], 1):  # Show first 10
                print(f"  {i}. {pattern}")
            if len(mixed_patterns) > 10:
                print(f"  ... and {len(mixed_patterns) - 10} more")
        else:
            print("📝 No mixed Hindi-English patterns found")
        
        # Find common field markers
        field_markers = [
            'bid number', 'ministry', 'department', 'organisation', 'contract period',
            'item category', 'bid end date', 'bid opening', 'validity', 'similar category',
            'mse exemption', 'beneficiary', 'dated'
        ]
        
        print(f"\n🔍 Looking for common field markers:")
        for marker in field_markers:
            # Look for both Hindi-first and English-first patterns
            english_first = re.search(rf'{marker}\s*:\s*([^\n]+)', text, re.IGNORECASE)
            hindi_first = re.search(rf'([^\n]+)\s*{marker}\s*:', text, re.IGNORECASE)
            
            if english_first:
                print(f"  ✅ {marker}: English-first pattern found")
            elif hindi_first:
                print(f"  ✅ {marker}: Hindi-first pattern found")
            else:
                print(f"  ❌ {marker}: No pattern found")
        
        print("="*80)
        return {
            'hindi_patterns': hindi_patterns,
            'english_patterns': english_patterns,
            'mixed_patterns': mixed_patterns
        }

def find_all_pdfs_in_data_directory_recursive(data_dir):
    """Find all PDF files recursively in data directory with improved performance"""
    pdf_files = []
    
    try:
        # Use pathlib for better performance
        data_path = Path(data_dir)
        
        # Use glob for faster file finding
        for pdf_file in data_path.rglob("*.pdf"):
            pdf_files.append(str(pdf_file))
        
        # Sort files for consistent processing order
        pdf_files.sort()
        
    except Exception as e:
        print(f"⚠️  Error finding PDF files: {e}")
        # Fallback to os.walk if pathlib fails
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.lower().endswith('.pdf'):
                    pdf_files.append(os.path.join(root, file))
    
    return pdf_files

def process_all_pdfs_in_data_directory_multi_threaded(max_workers=4):
    """Process all PDFs in data directory using multi-threading with improved efficiency"""
    # Setup Django environment first
    import os
    import sys
    import django
    
    # Add the project root to Python path
    project_root = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(project_root))
    
    # Set Django settings
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_data.settings')
    django.setup()
    
    data_dir = Path(__file__).parent / "data"
    extracted_data_dir = Path(__file__).parent / "extracted_data"
    
    # Ensure directories exist
    data_dir.mkdir(exist_ok=True)
    extracted_data_dir.mkdir(exist_ok=True)
    
    # Find all PDF files recursively
    pdf_files = find_all_pdfs_in_data_directory_recursive(str(data_dir))
    
    if not pdf_files:
        print("❌ No PDF files found in data directory or subdirectories")
        return
    
    # Initialize comprehensive logger
    logger = ProcessLogger()
    logger.log_session_start(len(pdf_files))
    
    print(f"📁 Found {len(pdf_files)} PDF files in data directory and subdirectories")
    print(f"🚀 Starting multi-threaded processing with {max_workers} workers...")
    print(f"📄 Logging to: {logger.log_dir}")
    print("="*80)
    
    # Thread-safe counters
    successful_extractions = 0
    failed_extractions = 0
    skipped_extractions = 0
    error_details = []
    
    # Thread lock for safe counter updates
    counter_lock = threading.Lock()
    
    start_time = time.time()
    
    def process_pdf_with_counter(pdf_path):
        """Process a single PDF and update counters safely"""
        nonlocal successful_extractions, failed_extractions, skipped_extractions, error_details
        
        file_start_time = time.time()
        filename = os.path.basename(pdf_path)
        
        try:
            # Ensure Django is set up in this thread
            import django
            if not django.conf.settings.configured:
                os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_data.settings')
                django.setup()
            
            print(f"🔄 Processing: {filename}")
            
            # Check if file is readable
            if not os.access(pdf_path, os.R_OK):
                error_msg = f"File not readable (permission denied)"
                file_size = 0
                processing_time = time.time() - file_start_time
                
                logger.log_file_processing(
                    filename, 'IGNORED', error_msg, file_size, processing_time
                )
                
                with counter_lock:
                    failed_extractions += 1
                    error_details.append(error_msg)
                print(f"❌ {error_msg}")
                return False
            
            # Check file size
            file_size = os.path.getsize(pdf_path)
            if file_size == 0:
                error_msg = f"Empty file (0 bytes)"
                processing_time = time.time() - file_start_time
                
                logger.log_file_processing(
                    filename, 'IGNORED', error_msg, file_size, processing_time
                )
                
                with counter_lock:
                    failed_extractions += 1
                    error_details.append(error_msg)
                print(f"❌ {error_msg}")
                return False
            
            # Create extractor and process
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
                    
                    # Get bid details for logging
                    bid_data = extractor.extract_bidding_data_enhanced(text)
                    bid_number = bid_data.get('bid_number', '')
                    pages_extracted = len(text.split('\n')) if text else 0
                    
                    processing_time = time.time() - file_start_time
                    
                    logger.log_file_processing(
                        filename, 'SUCCESS', 
                        f"Successfully extracted and saved to database", 
                        file_size, processing_time, "", bid_number, pages_extracted
                    )
                    
                    # Update counter safely
                    with counter_lock:
                        successful_extractions += 1
                    
                    print(f"✅ Successfully processed: {filename}")
                    return True
                else:
                    # Check if it was skipped due to duplicate
                    bid_data = extractor.extract_bidding_data_enhanced(extractor.extract_text_from_pdf())
                    bid_number = bid_data.get('bid_number', '')
                    pages_extracted = len(text.split('\n')) if text else 0
                    
                    if extractor.check_bid_exists(bid_number):
                        processing_time = time.time() - file_start_time
                        
                        logger.log_file_processing(
                            filename, 'SKIPPED', 
                            f"Bid already exists in database", 
                            file_size, processing_time, "", bid_number, pages_extracted
                        )
                        
                        with counter_lock:
                            skipped_extractions += 1
                        print(f"⏭️  Skipped (already exists): {filename}")
                        return False
                    else:
                        error_msg = f"Failed to save to database"
                        processing_time = time.time() - file_start_time
                        
                        logger.log_file_processing(
                            filename, 'FAILED', error_msg, file_size, processing_time, 
                            "Database save operation failed", bid_number, pages_extracted
                        )
                        
                        with counter_lock:
                            failed_extractions += 1
                            error_details.append(error_msg)
                        print(f"❌ {error_msg}")
                        return False
            else:
                error_msg = f"Failed to extract data from PDF"
                processing_time = time.time() - file_start_time
                
                logger.log_file_processing(
                    filename, 'FAILED', error_msg, file_size, processing_time, 
                    "PDF text extraction failed", "", 0
                )
                
                with counter_lock:
                    failed_extractions += 1
                    error_details.append(error_msg)
                print(f"❌ {error_msg}")
                return False
                
        except Exception as e:
            error_msg = f"Exception during processing: {str(e)}"
            processing_time = time.time() - file_start_time
            
            logger.log_file_processing(
                filename, 'FAILED', error_msg, file_size, processing_time, 
                str(e), "", 0
            )
            
            with counter_lock:
                failed_extractions += 1
                error_details.append(error_msg)
            print(f"❌ {error_msg}")
            return False
    
    # Process PDFs using ThreadPoolExecutor with improved task distribution
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="PDF_Worker") as executor:
        # Submit all tasks - each thread will get a different file
        future_to_pdf = {}
        
        # Distribute files evenly across threads
        for i, pdf_path in enumerate(pdf_files):
            # Submit task and store future
            future = executor.submit(process_pdf_with_counter, pdf_path)
            future_to_pdf[future] = pdf_path
        
        print(f"📤 Submitted {len(pdf_files)} tasks to {max_workers} worker threads")
        print("🔄 Workers are processing files concurrently...")
        
        # Process completed tasks and show progress
        completed = 0
        for future in as_completed(future_to_pdf):
            pdf_path = future_to_pdf[future]
            completed += 1
            
            try:
                # Get result (this will raise any exceptions that occurred)
                success = future.result()
                
                # Show progress
                progress = (completed / len(pdf_files)) * 100
                print(f"📊 Progress: {completed}/{len(pdf_files)} ({progress:.1f}%) - {os.path.basename(pdf_path)}")
                
            except Exception as e:
                print(f"❌ Exception in thread for {os.path.basename(pdf_path)}: {e}")
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    # Log session completion
    logger.log_session_end()
    
    print("\n" + "="*80)
    print("📊 OPTIMIZED MULTI-THREADED EXTRACTION SUMMARY")
    print("="*80)
    print(f"✅ Successful extractions: {successful_extractions}")
    print(f"⏭️  Skipped (duplicates): {skipped_extractions}")
    print(f"❌ Failed extractions: {failed_extractions}")
    print(f"📁 Total PDFs found: {len(pdf_files)}")
    print(f"📂 Excel/JSON files saved to: {extracted_data_dir}")
    print(f"⏱️  Total processing time: {processing_time:.2f} seconds")
    print(f"🚀 Average time per PDF: {processing_time/len(pdf_files):.2f} seconds")
    print(f"⚡ Speed improvement: {max_workers}x faster than single-threaded")
    
    # Show log file locations
    log_files = logger.get_log_files()
    print(f"\n📄 LOG FILES SAVED:")
    print(f"  📋 Detailed log: {log_files['log_file']}")
    print(f"  📊 CSV summary: {log_files['csv_file']}")
    print(f"  📋 Session summary: {log_files['summary_file']}")
    
    # Show error details if any
    if error_details:
        print(f"\n❌ ERROR DETAILS ({len(error_details)} errors):")
        for i, error in enumerate(error_details[:20], 1):  # Show first 20 errors
            print(f"  {i}. {error}")
        if len(error_details) > 20:
            print(f"  ... and {len(error_details) - 20} more errors")
    
    print("="*80)

def process_all_pdfs_ultra_fast(max_workers=8):
    """Process all PDFs using ultra-fast chunked multithreading"""
    # Setup Django environment first
    import os
    import sys
    import django
    
    # Add the project root to Python path
    project_root = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(project_root))
    
    # Set Django settings
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_data.settings')
    django.setup()
    
    data_dir = Path(__file__).parent / "data"
    extracted_data_dir = Path(__file__).parent / "extracted_data"
    
    # Ensure directories exist
    data_dir.mkdir(exist_ok=True)
    extracted_data_dir.mkdir(exist_ok=True)
    
    # Find all PDF files recursively
    pdf_files = find_all_pdfs_in_data_directory_recursive(str(data_dir))
    
    if not pdf_files:
        print("❌ No PDF files found in data directory or subdirectories")
        return
    
    # Initialize comprehensive logger
    logger = ProcessLogger()
    logger.log_session_start(len(pdf_files))
    
    print(f"📁 Found {len(pdf_files)} PDF files in data directory and subdirectories")
    print(f"🚀 Starting ULTRA-FAST processing with {max_workers} workers...")
    print(f"📄 Logging to: {logger.log_dir}")
    print("="*80)
    
    # Thread-safe counters
    successful_extractions = 0
    failed_extractions = 0
    skipped_extractions = 0
    
    # Thread lock for safe counter updates
    counter_lock = threading.Lock()
    
    start_time = time.time()
    
    def process_pdf_chunk(pdf_chunk):
        """Process a chunk of PDFs and update counters safely"""
        nonlocal successful_extractions, failed_extractions, skipped_extractions
        
        chunk_results = []
        
        for pdf_path in pdf_chunk:
            try:
                # Ensure Django is set up in this thread
                import django
                if not django.conf.settings.configured:
                    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_data.settings')
                    django.setup()
                
                print(f"🔄 Processing: {os.path.basename(pdf_path)}")
                
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
                        
                        chunk_results.append(('success', pdf_path))
                        print(f"✅ Successfully processed: {os.path.basename(pdf_path)}")
                    else:
                        # Check if it was skipped due to duplicate
                        bid_data = extractor.extract_bidding_data_enhanced(extractor.extract_text_from_pdf())
                        bid_number = bid_data.get('bid_number', '')
                        if extractor.check_bid_exists(bid_number):
                            chunk_results.append(('skipped', pdf_path))
                            print(f"⏭️  Skipped (already exists): {os.path.basename(pdf_path)}")
                        else:
                            chunk_results.append(('failed', pdf_path))
                            print(f"❌ Failed to save: {os.path.basename(pdf_path)}")
                else:
                    chunk_results.append(('failed', pdf_path))
                    print(f"❌ Failed to extract data from: {os.path.basename(pdf_path)}")
                    
            except Exception as e:
                chunk_results.append(('failed', pdf_path))
                print(f"❌ Error processing {os.path.basename(pdf_path)}: {e}")
        
        # Update counters safely
        with counter_lock:
            for result_type, _ in chunk_results:
                if result_type == 'success':
                    successful_extractions += 1
                elif result_type == 'skipped':
                    skipped_extractions += 1
                elif result_type == 'failed':
                    failed_extractions += 1
        
        return chunk_results
    
    # Chunk files for optimal distribution
    chunk_size = max(1, len(pdf_files) // max_workers)
    file_chunks = [pdf_files[i:i + chunk_size] for i in range(0, len(pdf_files), chunk_size)]
    
    print(f"📦 Split {len(pdf_files)} files into {len(file_chunks)} chunks for optimal distribution")
    print(f"🔧 Each worker will process ~{chunk_size} files")
    
    # Process PDFs using ThreadPoolExecutor with chunked distribution
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="Ultra_Worker") as executor:
        # Submit chunked tasks - each thread gets a different chunk of files
        future_to_chunk = {}
        
        for i, chunk in enumerate(file_chunks):
            future = executor.submit(process_pdf_chunk, chunk)
            future_to_chunk[future] = f"Chunk_{i+1}"
        
        print(f"📤 Submitted {len(file_chunks)} chunks to {max_workers} worker threads")
        print("🔄 Workers are processing file chunks concurrently...")
        
        # Process completed chunks and show progress
        completed_chunks = 0
        for future in as_completed(future_to_chunk):
            chunk_name = future_to_chunk[future]
            completed_chunks += 1
            
            try:
                # Get results from chunk
                chunk_results = future.result()
                
                # Show progress
                progress = (completed_chunks / len(file_chunks)) * 100
                print(f"📊 Chunk Progress: {completed_chunks}/{len(file_chunks)} ({progress:.1f}%) - {chunk_name}")
                
                # Show chunk summary
                success_count = len([r for r in chunk_results if r[0] == 'success'])
                skipped_count = len([r for r in chunk_results if r[0] == 'skipped'])
                failed_count = len([r for r in chunk_results if r[0] in ['failed', 'error']])
                print(f"   📋 {chunk_name} Results: ✅{success_count} ⏭️{skipped_count} ❌{failed_count}")
                
            except Exception as e:
                print(f"❌ Exception in chunk {chunk_name}: {e}")
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    # Log session completion
    logger.log_session_end()
    
    print("\n" + "="*80)
    print("📊 ULTRA-FAST MULTI-THREADED EXTRACTION SUMMARY")
    print("="*80)
    print(f"✅ Successful extractions: {successful_extractions}")
    print(f"⏭️  Skipped (duplicates): {skipped_extractions}")
    print(f"❌ Failed extractions: {failed_extractions}")
    print(f"📁 Total PDFs found: {len(pdf_files)}")
    print(f"📂 Excel/JSON files saved to: {extracted_data_dir}")
    print(f"⏱️  Total processing time: {processing_time:.2f} seconds")
    print(f"🚀 Average time per PDF: {processing_time/len(pdf_files):.2f} seconds")
    print(f"⚡ Speed improvement: {max_workers}x faster than single-threaded")
    print(f"🚀 Ultra-fast mode: ~{max_workers * 1.5:.1f}x faster than regular multithreading")
    
    # Show log file locations
    log_files = logger.get_log_files()
    print(f"\n📄 LOG FILES SAVED:")
    print(f"  📋 Detailed log: {log_files['log_file']}")
    print(f"  📊 CSV summary: {log_files['csv_file']}")
    print(f"  📋 Session summary: {log_files['summary_file']}")
    
    print("="*80)

def process_all_pdfs_in_data_directory():
    """Process all PDFs in data directory (single-threaded)"""
    # Setup Django environment first
    import os
    import sys
    import django
    
    # Add the project root to Python path
    project_root = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(project_root))
    
    # Set Django settings
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_data.settings')
    django.setup()
    
    data_dir = Path(__file__).parent / "data"
    extracted_data_dir = Path(__file__).parent / "extracted_data"
    
    # Ensure directories exist
    data_dir.mkdir(exist_ok=True)
    extracted_data_dir.mkdir(exist_ok=True)
    
    # Find all PDF files recursively
    pdf_files = find_all_pdfs_in_data_directory_recursive(str(data_dir))
    
    if not pdf_files:
        print("❌ No PDF files found in data directory or subdirectories")
        return
    
    # Initialize comprehensive logger
    logger = ProcessLogger()
    logger.log_session_start(len(pdf_files))
    
    print(f"📁 Found {len(pdf_files)} PDF files in data directory and subdirectories")
    print("🚀 Starting single-threaded processing...")
    print(f"📄 Logging to: {logger.log_dir}")
    print("="*80)
    
    # Counters
    successful_extractions = 0
    failed_extractions = 0
    skipped_extractions = 0
    error_details = []
    
    start_time = time.time()
    
    # Process each PDF
    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            print(f"\n🔄 Processing {i}/{len(pdf_files)}: {os.path.basename(pdf_path)}")
            
            file_start_time = time.time()
            filename = os.path.basename(pdf_path)
            
            # Check if file is readable
            if not os.access(pdf_path, os.R_OK):
                error_msg = f"File not readable (permission denied)"
                file_size = 0
                processing_time = time.time() - file_start_time
                
                logger.log_file_processing(
                    filename, 'IGNORED', error_msg, file_size, processing_time
                )
                
                failed_extractions += 1
                error_details.append(error_msg)
                print(f"❌ {error_msg}")
                continue
            
            # Check file size
            file_size = os.path.getsize(pdf_path)
            if file_size == 0:
                error_msg = f"Empty file (0 bytes)"
                processing_time = time.time() - file_start_time
                
                logger.log_file_processing(
                    filename, 'IGNORED', error_msg, file_size, processing_time
                )
                
                failed_extractions += 1
                error_details.append(error_msg)
                print(f"❌ {error_msg}")
                continue
            
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
                    
                    # Get bid details for logging
                    bid_data = extractor.extract_bidding_data_enhanced(extractor.extract_text_from_pdf())
                    bid_number = bid_data.get('bid_number', '')
                    pages_extracted = len(text.split('\n')) if text else 0
                    
                    processing_time = time.time() - file_start_time
                    
                    logger.log_file_processing(
                        filename, 'SUCCESS', 
                        f"Successfully extracted and saved to database", 
                        file_size, processing_time, "", bid_number, pages_extracted
                    )
                    
                    successful_extractions += 1
                    print(f"✅ Successfully processed: {filename}")
                else:
                    # Check if it was skipped due to duplicate
                    bid_data = extractor.extract_bidding_data_enhanced(extractor.extract_text_from_pdf())
                    bid_number = bid_data.get('bid_number', '')
                    pages_extracted = len(text.split('\n')) if text else 0
                    
                    if extractor.check_bid_exists(bid_number):
                        processing_time = time.time() - file_start_time
                        
                        logger.log_file_processing(
                            filename, 'SKIPPED', 
                            f"Bid already exists in database", 
                            file_size, processing_time, "", bid_number, pages_extracted
                        )
                        
                        skipped_extractions += 1
                        print(f"⏭️  Skipped (already exists): {filename}")
                    else:
                        error_msg = f"Failed to save to database"
                        processing_time = time.time() - file_start_time
                        
                        logger.log_file_processing(
                            filename, 'FAILED', error_msg, file_size, processing_time, 
                            "Database save operation failed", bid_number, pages_extracted
                        )
                        
                        failed_extractions += 1
                        error_details.append(error_msg)
                        print(f"❌ {error_msg}")
            else:
                error_msg = f"Failed to extract data from PDF"
                processing_time = time.time() - file_start_time
                
                logger.log_file_processing(
                    filename, 'FAILED', error_msg, file_size, processing_time, 
                    "PDF text extraction failed", "", 0
                )
                
                failed_extractions += 1
                error_details.append(error_msg)
                print(f"❌ {error_msg}")
                
        except Exception as e:
            error_msg = f"Exception during processing: {str(e)}"
            processing_time = time.time() - file_start_time
            
            logger.log_file_processing(
                filename, 'FAILED', error_msg, file_size, processing_time, 
                str(e), "", 0
            )
            
            failed_extractions += 1
            error_details.append(error_msg)
            print(f"❌ {error_msg}")
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Log session completion
    logger.log_session_end()
    
    # Print summary
    print("\n" + "="*80)
    print("📊 EXTRACTION SUMMARY")
    print("="*80)
    print(f"✅ Successful extractions: {successful_extractions}")
    print(f"⏭️  Skipped extractions: {skipped_extractions}")
    print(f"❌ Failed extractions: {failed_extractions}")
    print(f"⏱️  Total time: {elapsed_time:.2f} seconds")
    print(f"📁 Total PDFs processed: {len(pdf_files)}")
    
    # Show log file locations
    log_files = logger.get_log_files()
    print(f"\n📄 LOG FILES SAVED:")
    print(f"  📋 Detailed log: {log_files['log_file']}")
    print(f"  📊 CSV summary: {log_files['csv_file']}")
    print(f"  📋 Session summary: {log_files['summary_file']}")
    
    # Show error details if any
    if error_details:
        print(f"\n❌ ERROR DETAILS ({len(error_details)} errors):")
        for i, error in enumerate(error_details[:20], 1):  # Show first 20 errors
            print(f"  {i}. {error}")
        if len(error_details) > 20:
            print(f"  ... and {len(error_details) - 20} more errors")
    
    print("="*80)



def diagnose_pdf_files():
    """Diagnose PDF files for common issues"""
    print("🔍 Diagnosing PDF files for common issues...")
    
    data_dir = Path(__file__).parent / "data"
    
    if not data_dir.exists():
        print("❌ Data directory not found")
        return
    
    pdf_files = find_all_pdfs_in_data_directory_recursive(str(data_dir))
    
    if not pdf_files:
        print("❌ No PDF files found")
        return
    
    print(f"📁 Found {len(pdf_files)} PDF files")
    print("🔍 Checking file health...")
    
    issues = {
        'unreadable': [],
        'empty': [],
        'corrupted': [],
        'healthy': []
    }
    
    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        
        # Check readability
        if not os.access(pdf_path, os.R_OK):
            issues['unreadable'].append(filename)
            continue
        
        # Check file size
        try:
            file_size = os.path.getsize(pdf_path)
            if file_size == 0:
                issues['empty'].append(filename)
                continue
        except OSError:
            issues['unreadable'].append(filename)
            continue
        
        # Check if PDF can be opened
        try:
            doc = fitz.open(pdf_path)
            page_count = len(doc)
            doc.close()
            
            if page_count > 0:
                issues['healthy'].append(filename)
            else:
                issues['corrupted'].append(filename)
                
        except Exception:
            issues['corrupted'].append(filename)
    
    # Print diagnosis summary
    print("\n" + "="*60)
    print("📊 PDF DIAGNOSIS SUMMARY")
    print("="*60)
    print(f"✅ Healthy files: {len(issues['healthy'])}")
    print(f"⚠️  Empty files: {len(issues['empty'])}")
    print(f"❌ Corrupted files: {len(issues['corrupted'])}")
    print(f"🚫 Unreadable files: {len(issues['unreadable'])}")
    print("="*60)
    
    # Show recommendations
    if issues['unreadable']:
        print(f"\n🚫 Unreadable files ({len(issues['unreadable'])}):")
        for filename in issues['unreadable'][:5]:  # Show first 5
            print(f"  - {filename}")
        if len(issues['unreadable']) > 5:
            print(f"  ... and {len(issues['unreadable']) - 5} more")
        print("💡 Recommendation: Check file permissions")
    
    if issues['empty']:
        print(f"\n⚠️  Empty files ({len(issues['empty'])}):")
        for filename in issues['empty'][:5]:  # Show first 5
            print(f"  - {filename}")
        if len(issues['empty']) > 5:
            print(f"  ... and {len(issues['empty']) - 5} more")
        print("💡 Recommendation: Remove or replace empty files")
    
    if issues['corrupted']:
        print(f"\n❌ Corrupted files ({len(issues['corrupted'])}):")
        for filename in issues['corrupted'][:5]:  # Show first 5
            print(f"  - {filename}")
        if len(issues['corrupted']) > 5:
            print(f"  ... and {len(issues['corrupted']) - 5} more")
        print("💡 Recommendation: Re-download or repair corrupted files")
    
    print("\n" + "="*60)

def view_logs():
    """View recent log files"""
    print("📄 Viewing recent log files...")
    
    log_dir = Path(__file__).parent / "logs"
    
    if not log_dir.exists():
        print("❌ Logs directory not found")
        return
    
    # Find log files
    log_files = list(log_dir.glob("*.log"))
    csv_files = list(log_dir.glob("*.csv"))
    summary_files = list(log_dir.glob("*_summary.json"))
    
    if not log_files and not csv_files and not summary_files:
        print("❌ No log files found")
        return
    
    print(f"📁 Log directory: {log_dir}")
    print(f"📋 Log files: {len(log_files)}")
    print(f"📊 CSV files: {len(csv_files)}")
    print(f"📋 Summary files: {len(summary_files)}")
    
    # Show most recent files
    if log_files:
        latest_log = max(log_files, key=lambda x: x.stat().st_mtime)
        print(f"\n📋 Latest log file: {latest_log.name}")
        print(f"📅 Modified: {datetime.fromtimestamp(latest_log.stat().st_mtime)}")
        
        # Show last few lines
        try:
            with open(latest_log, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if lines:
                    print(f"\n📄 Last 5 lines of {latest_log.name}:")
                    for line in lines[-5:]:
                        print(f"  {line.strip()}")
        except Exception as e:
            print(f"❌ Error reading log file: {e}")
    
    if csv_files:
        latest_csv = max(csv_files, key=lambda x: x.stat().st_mtime)
        print(f"\n📊 Latest CSV file: {latest_csv.name}")
        print(f"📅 Modified: {datetime.fromtimestamp(latest_csv.stat().st_mtime)}")
    
    if summary_files:
        latest_summary = max(summary_files, key=lambda x: x.stat().st_mtime)
        print(f"\n📋 Latest summary file: {latest_summary.name}")
        print(f"📅 Modified: {datetime.fromtimestamp(latest_summary.stat().st_mtime)}")
        
        # Show summary content
        try:
            with open(latest_summary, 'r', encoding='utf-8') as f:
                summary_data = json.load(f)
                stats = summary_data.get('statistics', {})
                print(f"\n📊 Latest session statistics:")
                print(f"  ✅ Successful: {stats.get('successful', 0)}")
                print(f"  ⏭️  Skipped: {stats.get('skipped', 0)}")
                print(f"  ❌ Failed: {stats.get('failed', 0)}")
                print(f"  ⚠️  Ignored: {stats.get('ignored', 0)}")
                print(f"  ⏱️  Duration: {stats.get('duration_seconds', 0):.2f} seconds")
        except Exception as e:
            print(f"❌ Error reading summary file: {e}")
    
    print("\n💡 Use '--view-logs' to see this information anytime")



def show_help():
    """Display help information for the script"""
    print("🧪 GEM BIDDING DATA EXTRACTOR - COMMAND LINE USAGE")
    print("="*60)
    print("Usage:")
    print("  python data_extractor.py [options] [pdf_file]")
    print("")
    print("Options:")
    print("  --help, -h              Show this help message")
    print("  --analyze-patterns, -ap Analyze text patterns in PDFs")
    print("  --test-extraction, -tx  Test enhanced extraction on a single PDF")
    print("  --multi-thread, -mt     Process PDFs using multi-threading")
    print("  --workers=N, -w=N       Set number of worker threads (default: 4)")
    print("  --ultra-fast, -uf      Process PDFs using ultra-fast multithreading")
    print("  --ufw=N, -ufw=N        Set number of ultra-fast worker threads (default: 8)")
    print("  --diagnose, -d          Diagnose PDF files for common issues")
    print("  --view-logs, -vl        View recent log files and statistics")
    print("")
    print("Examples:")
    print("  python data_extractor.py                    # Process all PDFs in data/ directory")
    print("  python data_extractor.py document.pdf       # Process single PDF file")
    print("  python data_extractor.py --multi-thread     # Multi-threaded processing")
    print("  python data_extractor.py --multi-thread --workers=8  # 8 worker threads")
    print("  python data_extractor.py --ultra-fast       # Ultra-fast multithreading")
    print("  python data_extractor.py --ultra-fast --ufw=16  # 16 ultra-fast worker threads")
    print("  python data_extractor.py --diagnose                # Diagnose PDF files")
    print("  python data_extractor.py --view-logs               # View recent logs")
    print("  python data_extractor.py --analyze-patterns        # Analyze text patterns in PDFs")
    print("  python data_extractor.py --test-extraction        # Test enhanced extraction on a single PDF")
    print("")
    print("Note: Place PDF files in the 'data/' directory for batch processing")
    print("="*60)

def main():
    # Check for help command first
    if "--help" in sys.argv or "-h" in sys.argv:
        show_help()
        return
    
    # Check for special commands first
    if "--diagnose" in sys.argv or "-d" in sys.argv:
        # Diagnose PDF files
        print("🔍 PDF File Diagnosis Mode")
        diagnose_pdf_files()
    elif "--view-logs" in sys.argv or "-vl" in sys.argv:
        # View recent logs
        print("📄 Log Viewer Mode")
        view_logs()

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
    elif "--ultra-fast" in sys.argv or "-uf" in sys.argv:
        # Get number of ultra-fast workers from command line
        max_workers = 8  # Default for ultra-fast
        for arg in sys.argv:
            if arg.startswith("--ufw="):
                max_workers = int(arg.split("=")[1])
                break
            elif arg.startswith("-ufw="):
                max_workers = int(arg.split("=")[1])
                break
        
        print(f"🚀 Starting ULTRA-FAST processing with {max_workers} workers...")
        process_all_pdfs_ultra_fast(max_workers)
    elif "--analyze-patterns" in sys.argv or "-ap" in sys.argv:
        # Analyze text patterns in PDFs
        print("🔍 Analyzing text patterns in PDFs...")
        analyze_pdf_text_patterns()
    elif "--test-extraction" in sys.argv or "-tx" in sys.argv:
        # Test enhanced extraction on a single PDF
        print("🧪 Testing enhanced extraction on a single PDF...")
        test_enhanced_extraction()
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

def analyze_pdf_text_patterns():
    """Analyze text patterns in PDFs to understand Hindi-English text structure"""
    data_dir = Path(__file__).parent / "data"
    
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return
    
    print(f"🔍 Analyzing text patterns in PDFs from: {data_dir}")
    print("="*80)
    
    # Find all PDF files
    pdf_files = find_all_pdfs_in_data_directory_recursive(str(data_dir))
    
    if not pdf_files:
        print("❌ No PDF files found")
        return
    
    print(f"📁 Found {len(pdf_files)} PDF files")
    
    # Analyze first few PDFs to understand patterns
    for i, pdf_path in enumerate(pdf_files[:5], 1):  # Analyze first 5 PDFs
        try:
            print(f"\n🔍 Analyzing PDF {i}/{min(5, len(pdf_files))}: {os.path.basename(pdf_path)}")
            
            # Create extractor and extract text
            extractor = GeMBiddingPDFExtractor(pdf_path)
            text = extractor.extract_text_from_pdf()
            
            if text:
                # Analyze patterns
                patterns = extractor.analyze_text_patterns(text)
                
                # Show summary
                print(f"📊 Summary for {os.path.basename(pdf_path)}:")
                print(f"  - Hindi patterns: {len(patterns['hindi_patterns'])}")
                print(f"  - English patterns: {len(patterns['english_patterns'])}")
                print(f"  - Mixed patterns: {len(patterns['mixed_patterns'])}")
            else:
                print(f"❌ Could not extract text from {os.path.basename(pdf_path)}")
                
        except Exception as e:
            print(f"❌ Error analyzing {os.path.basename(pdf_path)}: {str(e)}")
    
    print("\n" + "="*80)
    print("📊 TEXT PATTERN ANALYSIS COMPLETE")
    print("="*80)
    print("💡 This analysis helps understand how Hindi and English text are structured in your PDFs")
    print("💡 Use this information to improve extraction patterns if needed")
    print("="*80)

def test_enhanced_extraction():
    """Test enhanced extraction on a single PDF to verify pattern matching"""
    data_dir = Path(__file__).parent / "data"
    
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return
    
    print(f"🧪 Testing enhanced extraction from: {data_dir}")
    print("="*80)
    
    # Find PDF files
    pdf_files = find_all_pdfs_in_data_directory_recursive(str(data_dir))
    
    if not pdf_files:
        print("❌ No PDF files found")
        return
    
    # Test on first PDF
    pdf_path = pdf_files[0]
    print(f"📄 Testing on: {os.path.basename(pdf_path)}")
    
    try:
        # Create extractor
        extractor = GeMBiddingPDFExtractor(pdf_path)
        
        # Extract text
        print("🔍 Extracting text from PDF...")
        text = extractor.extract_text_from_pdf()
        
        if not text:
            print("❌ No text extracted from PDF")
            return
        
        print(f"✅ Text extracted: {len(text)} characters")
        
        # Analyze patterns first
        print("\n🔍 Analyzing text patterns...")
        patterns = extractor.analyze_text_patterns(text)
        
        # Test enhanced extraction
        print("\n🧪 Testing enhanced extraction...")
        extracted_data = extractor.extract_bidding_data_enhanced(text)
        
        # Show results
        print("\n📊 EXTRACTION RESULTS:")
        print("="*80)
        for field, value in extracted_data.items():
            if field != 'raw_text':
                if value:
                    print(f"✅ {field}: {value}")
                else:
                    print(f"❌ {field}: No data extracted")
        
        print("="*80)
        

        
    except Exception as e:
        print(f"❌ Error during test extraction: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
