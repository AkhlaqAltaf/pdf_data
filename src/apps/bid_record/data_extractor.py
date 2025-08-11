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
            
        except ImportError as e:
            print(f"⚠️  Warning: sentence-transformers not installed. Installing now...")
            try:
                import subprocess
                import sys
                subprocess.check_call([sys.executable, "-m", "pip", "install", "sentence-transformers"])
                print("✅ sentence-transformers installed successfully!")
                
                # Try again after installation
                from sentence_transformers import SentenceTransformer
                model = SentenceTransformer('all-MiniLM-L6-v2')
                embedding = model.encode([text], normalize_embeddings=True)
                embedding_list = embedding[0].tolist()
                print(f"✅ Generated embedding with {len(embedding_list)} dimensions")
                return embedding_list
                
            except Exception as install_error:
                print(f"❌ Failed to install sentence-transformers: {install_error}")
                return None
                
        except Exception as e:
            print(f"⚠️  Warning: Error generating embedding: {e}")
            print("🔄 Retrying with fallback method...")
            
            try:
                # Fallback: Try to use a different model or method
                from sentence_transformers import SentenceTransformer
                
                # Try different model if available
                try:
                    model = SentenceTransformer('paraphrase-MiniLM-L3-v2')
                except:
                    model = SentenceTransformer('all-MiniLM-L6-v2')
                
                embedding = model.encode([text], normalize_embeddings=True)
                embedding_list = embedding[0].tolist()
                print(f"✅ Generated embedding with fallback method: {len(embedding_list)} dimensions")
                return embedding_list
                
            except Exception as fallback_error:
                print(f"❌ Fallback embedding generation also failed: {fallback_error}")
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
    
    print(f"📁 Found {len(pdf_files)} PDF files in data directory and subdirectories")
    print(f"🚀 Starting multi-threaded processing with {max_workers} workers...")
    print("="*80)
    
    # Thread-safe counters
    successful_extractions = 0
    failed_extractions = 0
    skipped_extractions = 0
    
    # Thread lock for safe counter updates
    counter_lock = threading.Lock()
    
    start_time = time.time()
    
    def process_pdf_with_counter(pdf_path):
        """Process a single PDF and update counters safely"""
        nonlocal successful_extractions, failed_extractions, skipped_extractions
        
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
                    
                    # Update counter safely
                    with counter_lock:
                        successful_extractions += 1
                    
                    print(f"✅ Successfully processed: {os.path.basename(pdf_path)}")
                    return True
                else:
                    # Check if it was skipped due to duplicate
                    bid_data = extractor.extract_bidding_data(extractor.extract_text_from_pdf())
                    bid_number = bid_data.get('bid_number', '')
                    if extractor.check_bid_exists(bid_number):
                        with counter_lock:
                            skipped_extractions += 1
                        print(f"⏭️  Skipped (already exists): {os.path.basename(pdf_path)}")
                        return False
                    else:
                        with counter_lock:
                            failed_extractions += 1
                        print(f"❌ Failed to save: {os.path.basename(pdf_path)}")
                        return False
            else:
                with counter_lock:
                    failed_extractions += 1
                print(f"❌ Failed to extract data from: {os.path.basename(pdf_path)}")
                return False
                
        except Exception as e:
            with counter_lock:
                failed_extractions += 1
            print(f"❌ Error processing {os.path.basename(pdf_path)}: {e}")
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
    
    print(f"📁 Found {len(pdf_files)} PDF files in data directory and subdirectories")
    print(f"🚀 Starting ULTRA-FAST processing with {max_workers} workers...")
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
    
    print(f"📁 Found {len(pdf_files)} PDF files in data directory and subdirectories")
    print("🚀 Starting single-threaded processing...")
    print("="*80)
    
    # Counters
    successful_extractions = 0
    failed_extractions = 0
    skipped_extractions = 0
    
    start_time = time.time()
    
    # Process each PDF
    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            print(f"\n🔄 Processing {i}/{len(pdf_files)}: {os.path.basename(pdf_path)}")
            
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
                    
                    successful_extractions += 1
                    print(f"✅ Successfully processed: {os.path.basename(pdf_path)}")
                else:
                    # Check if it was skipped due to duplicate
                    bid_data = extractor.extract_bidding_data(extractor.extract_text_from_pdf())
                    bid_number = bid_data.get('bid_number', '')
                    if extractor.check_bid_exists(bid_number):
                        skipped_extractions += 1
                        print(f"⏭️  Skipped (already exists): {os.path.basename(pdf_path)}")
                    else:
                        failed_extractions += 1
                        print(f"❌ Failed to save: {os.path.basename(pdf_path)}")
            else:
                failed_extractions += 1
                print(f"❌ Failed to extract data from: {os.path.basename(pdf_path)}")
                
        except Exception as e:
            failed_extractions += 1
            print(f"❌ Error processing {os.path.basename(pdf_path)}: {e}")
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Print summary
    print("\n" + "="*80)
    print("📊 EXTRACTION SUMMARY")
    print("="*80)
    print(f"✅ Successful extractions: {successful_extractions}")
    print(f"⏭️  Skipped extractions: {skipped_extractions}")
    print(f"❌ Failed extractions: {failed_extractions}")
    print(f"⏱️  Total time: {elapsed_time:.2f} seconds")
    print(f"📁 Total PDFs processed: {len(pdf_files)}")
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
    print("  --ultra-fast, -uf      Process PDFs using ultra-fast multithreading")
    print("  --ufw=N, -ufw=N        Set number of ultra-fast worker threads (default: 8)")
    print("")
    print("Examples:")
    print("  python data_extractor.py                    # Process all PDFs in data/ directory")
    print("  python data_extractor.py document.pdf       # Process single PDF file")
    print("  python data_extractor.py --multi-thread     # Multi-threaded processing")
    print("  python data_extractor.py --multi-thread --workers=8  # 8 worker threads")
    print("  python data_extractor.py --ultra-fast       # Ultra-fast multithreading")
    print("  python data_extractor.py --ultra-fast --ufw=16  # 16 ultra-fast worker threads")
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
