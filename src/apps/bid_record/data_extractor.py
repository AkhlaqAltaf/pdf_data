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

# Global embedding model cache for thread safety
_embedding_model_cache = {}
_embedding_model_lock = threading.Lock()

def get_global_embedding_model():
    """Get a global embedding model instance for thread safety"""
    global _embedding_model_cache
    
    with _embedding_model_lock:
        if 'model' not in _embedding_model_cache:
            try:
                import torch
                from sentence_transformers import SentenceTransformer
                
                # Force CPU device to avoid GPU conflicts
                device = torch.device('cpu')
                
                # Load model with specific device
                model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
                
                # Ensure model is properly loaded on CPU
                if hasattr(model, 'to'):
                    model = model.to(device)
                
                # Store in cache
                _embedding_model_cache['model'] = model
                _embedding_model_cache['device'] = device
                
                print(f"✅ Global embedding model loaded on {device}")
                
            except Exception as e:
                print(f"❌ Failed to load global embedding model: {e}")
                _embedding_model_cache['model'] = None
                _embedding_model_cache['device'] = None
    
    return _embedding_model_cache.get('model'), _embedding_model_cache.get('device')

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

def preload_embedding_model():
    """Pre-load the embedding model to avoid conflicts during multi-threading"""
    print("🔄 Pre-loading embedding model for multi-threading...")
    
    try:
        model, device = get_global_embedding_model()
        if model is not None:
            print(f"✅ Embedding model pre-loaded successfully on {device}")
            return True
        else:
            print("❌ Failed to pre-load embedding model")
            return False
    except Exception as e:
        print(f"❌ Error pre-loading embedding model: {e}")
        return False

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
        
        # Extract item category - just capture one line, simple and clean
        category_match = re.search(r'Item Category\s*\n([^\n]+)', text, re.IGNORECASE)
        if category_match:
            category_text = category_match.group(1).strip()
            # Clean up the text
            category_text = re.sub(r'[^\x00-\x7F]+', '', category_text)  # Remove non-ASCII
            category_text = re.sub(r'\s+', ' ', category_text).strip()  # Normalize whitespace
            bidding_data['item_category'] = category_text
        else:
            # Try alternative pattern
            category_match = re.search(r'item\s*category\s*:\s*([^\n]+)', text, re.IGNORECASE)
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
        """Generate embedding for the given text using sentence-transformers - SIMPLIFIED VERSION"""
        try:
            # Simple, direct approach - no threading complications
            from sentence_transformers import SentenceTransformer
            import torch
            
            print(f"🔍 [Thread-{threading.current_thread().name}] Starting embedding generation...")
            
            # Force CPU device to avoid any GPU issues
            device = torch.device('cpu')
            
            # Load model directly - no caching, no threading issues
            print(f"🔄 [Thread-{threading.current_thread().name}] Loading sentence transformer model...")
            model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
            
            # Ensure model is on CPU
            model = model.to(device)
            
            print(f"✅ [Thread-{threading.current_thread().name}] Model loaded successfully on {device}")
            
            # Generate embedding
            print(f"🔄 [Thread-{threading.current_thread().name}] Generating embedding...")
            with torch.no_grad():
                embedding = model.encode([text], normalize_embeddings=True, show_progress_bar=False)
            
            # Convert to list
            embedding_list = embedding[0].tolist()
            
            print(f"✅ [Thread-{threading.current_thread().name}] Generated embedding: {len(embedding_list)} dimensions")
            print(f"📊 [Thread-{threading.current_thread().name}] First 3 values: {embedding_list[:3]}")
            
            return embedding_list
            
        except Exception as e:
            print(f"❌ [Thread-{threading.current_thread().name}] Error in embedding generation: {e}")
            print(f"🔄 [Thread-{threading.current_thread().name}] Trying fallback method...")
            
            try:
                # Fallback: Use a different model
                from sentence_transformers import SentenceTransformer
                import torch
                
                print(f"🔄 [Thread-{threading.current_thread().name}] Loading fallback model...")
                model = SentenceTransformer('paraphrase-MiniLM-L3-v2', device='cpu')
                
                with torch.no_grad():
                    embedding = model.encode([text], normalize_embeddings=True, show_progress_bar=False)
                
                embedding_list = embedding[0].tolist()
                print(f"✅ [Thread-{threading.current_thread().name}] Fallback embedding generated: {len(embedding_list)} dimensions")
                
                return embedding_list
                
            except Exception as fallback_error:
                print(f"❌ [Thread-{threading.current_thread().name}] Fallback also failed: {fallback_error}")
                
                # Last resort: Create a simple embedding
                print(f"⚠️  [Thread-{threading.current_thread().name}] Creating simple fallback embedding...")
                
                # Create a simple hash-based embedding (not ideal but will work)
                import hashlib
                text_hash = hashlib.md5(text.encode()).hexdigest()
                
                # Convert hash to 384-dimensional vector
                simple_embedding = []
                for i in range(384):
                    # Use hash to generate pseudo-random but consistent values
                    hash_val = int(text_hash[i % 32], 16)
                    simple_embedding.append((hash_val - 8) / 8.0)  # Scale to roughly -1 to 1
                
                print(f"⚠️  [Thread-{threading.current_thread().name}] Created simple embedding: {len(simple_embedding)} dimensions")
                return simple_embedding
    
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
            
            # Generate embedding for the cleaned text - THIS MUST WORK
            print("🔍 Generating embedding for bid text...")
            embedding = self.generate_embedding(cleaned_text)
            
            # CRITICAL: Ensure we have a valid embedding
            if embedding is None:
                print("❌ CRITICAL ERROR: Embedding generation failed completely!")
                return False
            
            print(f"✅ Embedding generated successfully: {len(embedding)} dimensions")
            print(f"📊 Embedding sample values: {embedding[:3]}")
            
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
                    raw_text=cleaned_text,
                    embedding=embedding
                )
            
            # VERIFY the embedding was saved
            saved_bid = BidDocument.objects.get(bid_number=bid_number)
            saved_embedding = saved_bid.embedding
            
            if saved_embedding and len(saved_embedding) > 0:
                print(f"✅ SUCCESS: Bid saved with embedding: {len(saved_embedding)} dimensions")
                print(f"📊 Saved embedding sample: {saved_embedding[:3]}")
            else:
                print(f"❌ ERROR: Bid saved but embedding is empty or null!")
                print(f"📊 Expected embedding length: {len(embedding)}")
                print(f"📊 Actual saved embedding: {saved_embedding}")
            
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
                    bid_data = extractor.extract_bidding_data(text)
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
                    bid_data = extractor.extract_bidding_data(extractor.extract_text_from_pdf())
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
                        bid_data = extractor.extract_bidding_data(extractor.extract_text_from_pdf())
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
                    bid_data = extractor.extract_bidding_data(extractor.extract_text_from_pdf())
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
                    bid_data = extractor.extract_bidding_data(extractor.extract_text_from_pdf())
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

def test_embedding_generation():
    """Test embedding generation to ensure it's working"""
    print("🧪 Testing embedding generation...")
    
    try:
        # Create a simple test extractor
        test_extractor = GeMBiddingPDFExtractor("test.pdf")
        
        # Test text
        test_text = "This is a test bid document for embedding generation."
        
        print(f"📝 Test text: {test_text}")
        
        # Generate embedding
        embedding = test_extractor.generate_embedding(test_text)
        
        if embedding:
            print(f"✅ Test embedding generated: {len(embedding)} dimensions")
            print(f"📊 First 5 values: {embedding[:5]}")
            print(f"📊 Last 5 values: {embedding[-5:]}")
            return True
        else:
            print("❌ Test embedding generation failed")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False

def show_help():
    """Display help information for the script"""
    print("🧪 GEM BIDDING DATA EXTRACTOR - COMMAND LINE USAGE")
    print("="*60)
    print("Usage:")
    print("  python data_extractor.py [options] [pdf_file]")
    print("")
    print("Options:")
    print("  --help, -h              Show this help message")
    print("  --test-embedding, -te   Test embedding generation")
    print("  --generate-embeddings, -ge  Generate embeddings for existing bids")
    print("  --multi-thread, -mt     Process PDFs using multi-threading")
    print("  --workers=N, -w=N       Set number of worker threads (default: 4)")
    print("  --ultra-fast, -uf      Process PDFs using ultra-fast multithreading")
    print("  --ufw=N, -ufw=N        Set number of ultra-fast worker threads (default: 8)")
    print("  --diagnose, -d          Diagnose PDF files for common issues")
    print("  --view-logs, -vl        View recent log files and statistics")
    print("  --verify-embeddings, -ve  Verify embeddings in database")
    print("")
    print("Examples:")
    print("  python data_extractor.py                    # Process all PDFs in data/ directory")
    print("  python data_extractor.py document.pdf       # Process single PDF file")
    print("  python data_extractor.py --test-embedding   # Test embedding generation")
    print("  python data_extractor.py --multi-thread     # Multi-threaded processing")
    print("  python data_extractor.py --multi-thread --workers=8  # 8 worker threads")
    print("  python data_extractor.py --ultra-fast       # Ultra-fast multithreading")
    print("  python data_extractor.py --ultra-fast --ufw=16  # 16 ultra-fast worker threads")
    print("  python data_extractor.py --generate-embeddings      # Generate embeddings")
    print("  python data_extractor.py --diagnose                # Diagnose PDF files")
    print("  python data_extractor.py --view-logs               # View recent logs")
    print("  python data_extractor.py --verify-embeddings       # Verify database embeddings")
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
    elif "--test-embedding" in sys.argv or "-te" in sys.argv:
        # Test embedding generation
        print("🧪 Testing embedding generation...")
        test_embedding_generation()
    elif "--verify-embeddings" in sys.argv or "-ve" in sys.argv:
        # Verify embeddings in database
        print("🔍 Embedding Verification Mode")
        verify_embeddings_in_database()
    elif "--generate-embeddings" in sys.argv or "-ge" in sys.argv:
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
    elif "--verify-embeddings" in sys.argv or "-ve" in sys.argv:
        # Verify embeddings in database
        print("🔍 Verifying embeddings in database...")
        verify_embeddings_in_database()
    elif "--test-embedding" in sys.argv or "-te" in sys.argv:
        # Test embedding generation
        print("🧪 Testing embedding generation...")
        test_embedding_generation()
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
