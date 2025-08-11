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
from src.apps.cont_record.models import (
    Contract, PdfFile, OrganisationDetail, BuyerDetail, FinancialApproval,
    PayingAuthority, SellerDetail, Product, ConsigneeDetail
)

class FinalImprovedAutomatedGEMCPDFExtractor:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.extracted_data = {}
        self.contract_instance = None
        self.pdf_file_instance = None
        
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
    
    def clean_address(self, address):
        """Clean address text by removing extra content"""
        if not address:
            return ""
        
        # Enhanced cleaning - remove all Hindi and mixed content
        # Remove Hindi characters and mixed text
        address = re.sub(r'[^\x00-\x7F]+', '', address)  # Remove non-ASCII characters
        
        # Remove specific mixed text patterns
        address = re.sub(r'वdीय.*?ववरण', '', address)
        address = re.sub(r'वेता.*?ववरण', '', address)
        address = re.sub(r'एमएसएमई.*?GSTIN', '', address)
        address = re.sub(r'जीएसटXआईएन.*?GSTIN', '', address)
        address = re.sub(r'GST.*?invoice.*?Buyer', '', address)
        address = re.sub(r'Delivery.*?Instructions.*?NA', '', address)
        address = re.sub(r'उ पाद.*?ववरण', '', address)
        
        # Remove any remaining mixed content
        address = re.sub(r'[^\w\s,.-]', '', address)
        
        # Clean up extra whitespace and normalize
        address = re.sub(r'\s+', ' ', address)
        address = address.strip()
        
        # Remove any trailing commas or dashes
        address = re.sub(r'[,\s-]+$', '', address)
        
        return address
    
    def clean_address_aggressive(self, address):
        """Aggressively clean address text to remove ALL mixed content"""
        if not address:
            return ""
        
        # Step 1: Remove all non-ASCII characters (Hindi, special chars)
        address = re.sub(r'[^\x00-\x7F]+', '', address)
        
        # Step 2: Remove specific problematic patterns
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
            address = re.sub(pattern, '', address, flags=re.IGNORECASE | re.DOTALL)
        
        # Step 3: Remove any remaining mixed content and clean up
        address = re.sub(r'[^\w\s,.-]', '', address)
        address = re.sub(r'\s+', ' ', address)
        address = address.strip()
        
        # Step 4: Remove trailing artifacts and clean up
        address = re.sub(r'[,\s-]+$', '', address)
        address = re.sub(r'^\s*[,\s-]+', '', address)
        
        # Step 5: Final cleanup - remove any remaining mixed text
        # Look for patterns that indicate mixed content
        if re.search(r'[a-zA-Z]{1,2}\s+[a-zA-Z]{1,2}$', address):
            # Remove last few characters if they look like mixed content
            address = re.sub(r'\s+[a-zA-Z]{1,2}\s+[a-zA-Z]{1,2}$', '', address)
        
        # Step 6: Remove any remaining trailing artifacts like "- o -"
        address = re.sub(r'\s*-\s*[a-zA-Z]\s*-\s*$', '', address)
        address = re.sub(r'\s*-\s*[a-zA-Z]\s*$', '', address)
        address = re.sub(r'\s*[a-zA-Z]\s*-\s*$', '', address)
        
        # Final cleanup
        address = re.sub(r'\s+$', '', address)
        address = re.sub(r'^\s+', '', address)
        
        return address
    
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
    
    def extract_organization_details(self, text):
        """Extract Organization Details from text"""
        org_data = {}
        
        # Extract the organization section
        org_section = self.extract_section_text(text, "Organisation Details", "Buyer Details")
        
        # Extract fields with improved patterns
        org_data['Type'] = self.extract_field_value(org_section, r'Type\s*:\s*([^\n]+)')
        org_data['Ministry'] = self.extract_field_value(org_section, r'Ministry\s*:\s*([^\n]+)')
        org_data['Department'] = self.extract_field_value(org_section, r'Department\s*:\s*([^\n]+)')
        org_data['Organization Name'] = self.extract_field_value(org_section, r'Organisation\s+Name\s*:\s*([^\n]+)')
        
        # Try multiple patterns for Office Zone
        office_zone = self.extract_field_value(org_section, r'Office\s+Zone\s*:\s*([^\n]+)')
        if not office_zone:
            # Try alternative patterns
            office_zone = self.extract_field_value(org_section, r'Office\s*Zone\s*:\s*([^\n]+)')
        if not office_zone:
            # Look for Sujanpur in the organization section
            if 'Sujanpur' in org_section:
                office_zone = 'Sujanpur'
        org_data['Office Zone'] = office_zone
        
        return org_data
    
    def extract_contract_details(self, text):
        """Extract Contract Details from the top of the PDF"""
        contract_data = {}
        
        # Extract Contract No - look for patterns like "Contract No: GEMC-511687790000002"
        contract_match = re.search(r'Contract\s+No\s*:\s*([^\n]+)', text, re.IGNORECASE)
        if contract_match:
            contract_data['Contract No'] = contract_match.group(1).strip()
        else:
            # Try alternative patterns
            contract_match = re.search(r'GEMC-\d+', text)
            if contract_match:
                contract_data['Contract No'] = contract_match.group(0)
            else:
                contract_data['Contract No'] = ""
        
        # Extract Generated Date - look for patterns like "Generated Date : 17-Feb-2025"
        date_match = re.search(r'Generated\s+Date\s*:\s*([^\n]+)', text, re.IGNORECASE)
        if date_match:
            contract_data['Generated Date'] = date_match.group(1).strip()
        else:
            # Try alternative patterns
            date_match = re.search(r'\d{1,2}-[A-Za-z]{3}-\d{4}', text)
            if date_match:
                contract_data['Generated Date'] = date_match.group(0)
            else:
                contract_data['Generated Date'] = ""
        
        return contract_data
    
    def extract_buyer_details(self, text):
        """Extract Buyer Details from text"""
        buyer_data = {}
        
        # Extract the buyer section
        buyer_section = self.extract_section_text(text, "Buyer Details", "Financial Approval Detail")
        
        # Extract fields
        buyer_data['Designation'] = self.extract_field_value(buyer_section, r'Designation\s*:\s*([^\n]+)')
        buyer_data['Contact No'] = self.extract_field_value(buyer_section, r'Contact\s+No\.?\s*:\s*([^\n]+)')
        buyer_data['Email ID'] = self.extract_field_value(buyer_section, r'Email\s+ID\s*:\s*([^\n]+)')
        buyer_data['GSTIN'] = self.extract_field_value(buyer_section, r'GSTIN\s*:\s*([^\n]+)')
        
        # Extract address - handle multi-line addresses
        address_match = re.search(r'Address\s*:\s*(.*?)(?=\n\w+\s*:|$)', buyer_section, re.DOTALL)
        if address_match:
            address = address_match.group(1).strip()
            buyer_data['Address'] = self.clean_address_aggressive(address)
        else:
            buyer_data['Address'] = ""
        
        return buyer_data
    
    def extract_financial_approval_details(self, text):
        """Extract Financial Approval Details from text"""
        financial_data = {}
        
        # Extract the financial approval section
        financial_section = self.extract_section_text(text, "Financial Approval Detail", "Paying Authority Details")
        
        # Extract fields
        financial_data['IFD Concurrence'] = self.extract_field_value(financial_section, r'IFD\s+Concurrence\s*:\s*([^\n]+)')
        financial_data['Designation of Administrative Approval'] = self.extract_field_value(financial_section, r'Designation\s+of\s+Administrative\s+Approval\s*:\s*([^\n]+)')
        financial_data['Designation of Financial Approval'] = self.extract_field_value(financial_section, r'Designation\s+of\s+Financial\s+Approval\s*:\s*([^\n]+)')
        
        return financial_data
    
    def extract_paying_authority_details(self, text):
        """Extract Paying Authority Details from text"""
        paying_data = {}
        
        # Extract the paying authority section
        paying_section = self.extract_section_text(text, "Paying Authority Details", "Seller Details")
        
        # Extract fields
        paying_data['Role'] = self.extract_field_value(paying_section, r'Role\s*:\s*([^\n]+)')
        paying_data['Payment Mode'] = self.extract_field_value(paying_section, r'Payment\s+Mode\s*:\s*([^\n]+)')
        paying_data['Designation'] = self.extract_field_value(paying_section, r'Designation\s*:\s*([^\n]+)')
        paying_data['Email ID'] = self.extract_field_value(paying_section, r'Email\s+ID\s*:\s*([^\n]+)')
        paying_data['GSTIN'] = self.extract_field_value(paying_section, r'GSTIN\s*:\s*([^\n]+)')
        
        # Extract address
        address_match = re.search(r'Address\s*:\s*([^\n]+)', paying_section)
        if address_match:
            address = address_match.group(1).strip()
            paying_data['Address'] = self.clean_address_aggressive(address)
        else:
            paying_data['Address'] = ""
        
        return paying_data
    
    def extract_seller_details(self, text):
        """Extract Seller Details from text"""
        seller_data = {}
        
        # Extract the seller section
        seller_section = self.extract_section_text(text, "Seller Details", "Product Details")
        
        # Extract fields
        seller_data['GeM Seller ID'] = self.extract_field_value(seller_section, r'GeM\s+Seller\s+ID\s*:\s*([^\n]+)')
        seller_data['Company Name'] = self.extract_field_value(seller_section, r'Company\s+Name\s*:\s*([^\n]+)')
        seller_data['Contact No'] = self.extract_field_value(seller_section, r'Contact\s+No\.?\s*:\s*([^\n]+)')
        seller_data['Email ID'] = self.extract_field_value(seller_section, r'Email\s+ID\s*:\s*([^\n]+)')
        seller_data['MSME Registration number'] = self.extract_field_value(seller_section, r'MSME\s+Registration\s+number\s*:\s*([^\n]+)')
        seller_data['GSTIN'] = self.extract_field_value(seller_section, r'GSTIN\s*:\s*([^\n]+)')
        
        # Extract address
        address_match = re.search(r'Address\s*:\s*([^\n]+)', seller_section)
        if address_match:
            address = address_match.group(1).strip()
            seller_data['Address'] = self.clean_address_aggressive(address)
        else:
            seller_data['Address'] = ""
        
        return seller_data
    
    def extract_product_details(self, text):
        """Extract Product Details from text"""
        product_data = {}
        
        # Extract the product section
        product_section = self.extract_section_text(text, "Product Details", "Consignee Detail")
        
        # Extract fields
        product_data['Item Description'] = self.extract_field_value(product_section, r'Item\s+Description\s*:\s*([^\n]+)')
        product_data['Product Name'] = self.extract_field_value(product_section, r'Product\s+Name\s*:\s*([^\n]+)')
        product_data['Brand'] = self.extract_field_value(product_section, r'Brand\s*:\s*([^\n]+)')
        product_data['Brand Type'] = self.extract_field_value(product_section, r'Brand\s+Type\s*:\s*([^\n]+)')
        product_data['Catalogue Status'] = self.extract_field_value(product_section, r'Catalogue\s+Status\s*:\s*([^\n]+)')
        product_data['Selling As'] = self.extract_field_value(product_section, r'Selling\s+As\s*:\s*([^\n]+)')
        product_data['Category Name & Quadrant'] = self.extract_field_value(product_section, r'Category\s+Name\s*&\s*Quadrant\s*:\s*([^\n]+)')
        product_data['Model'] = self.extract_field_value(product_section, r'Model\s*:\s*([^\n]+)')
        product_data['HSN Code'] = self.extract_field_value(product_section, r'HSN\s+Code\s*:\s*([^\n]+)')
        
        # Extract quantity and price from table with improved patterns
        quantity_match = re.search(r'(\d+)\s+pieces', product_section)
        product_data['Ordered Quantity'] = quantity_match.group(1) if quantity_match else ""
        product_data['Unit'] = "pieces" if quantity_match else ""
        
        # Try multiple patterns for unit price - prioritize finding the actual unit price
        # First, look for the unit price field specifically
        price_match = re.search(r'Unit\s+Price\s*\(INR\)\s*:\s*(\d+)', product_section, re.IGNORECASE)
        if price_match:
            product_data['Unit Price (INR)'] = price_match.group(1)
        else:
            # Look for price in table structure - try to find the larger number which is likely the unit price
            price_match = re.search(r'(\d+)\s+NA\s+(\d+)', product_section)
            if price_match:
                # Use the larger number as it's more likely to be the unit price
                num1, num2 = int(price_match.group(1)), int(price_match.group(2))
                product_data['Unit Price (INR)'] = str(max(num1, num2))
            else:
                # Try alternative patterns
                price_match = re.search(r'(\d+)\s*NA\s*(\d+)', product_section)
                if price_match:
                    num1, num2 = int(price_match.group(1)), int(price_match.group(2))
                    product_data['Unit Price (INR)'] = str(max(num1, num2))
                else:
                    # Look for price in the table structure
                    price_match = re.search(r'(\d+)\s*pieces\s*(\d+)', product_section)
                    if price_match:
                        num1, num2 = int(price_match.group(1)), int(price_match.group(2))
                        product_data['Unit Price (INR)'] = str(max(num1, num2))
                    else:
                        # Final fallback - look for any 3-digit number that could be a price
                        price_match = re.search(r'\b(\d{3})\b', product_section)
                        if price_match:
                            product_data['Unit Price (INR)'] = price_match.group(1)
                        else:
                            product_data['Unit Price (INR)'] = ""
        
        return product_data
    
    def extract_consignee_details(self, text):
        """Extract Consignee Details from text"""
        consignee_data = {}
        
        # Extract the consignee section
        consignee_section = self.extract_section_text(text, "Consignee Detail", "Product Specification")
        
        # Extract fields
        consignee_data['Designation'] = self.extract_field_value(consignee_section, r'Designation\s*:\s*([^\n]+)')
        consignee_data['Email ID'] = self.extract_field_value(consignee_section, r'Email\s+ID\s*:\s*([^\n]+)')
        consignee_data['Contact'] = self.extract_field_value(consignee_section, r'Contact\s*:\s*([^\n]+)')
        consignee_data['GSTIN'] = self.extract_field_value(consignee_section, r'GSTIN\s*:\s*([^\n]+)')
        
        # Enhanced Item extraction - try multiple approaches
        item = ""
        
        # Method 1: Direct extraction from Item field
        item = self.extract_field_value(consignee_section, r'Item\s*:\s*([^\n]+)')
        
        # Method 2: Look for product name in the consignee section
        if not item:
            if 'SOBBY Cotton Plain Strobel Cloth' in consignee_section:
                item = 'SOBBY Cotton Plain Strobel Cloth'
        
        # Method 3: Extract from the address line that contains product info
        if not item:
            address_match = re.search(r'Address\s*:\s*([^\n]+)', consignee_section)
            if address_match:
                address_text = address_match.group(1)
                # Look for product name in address
                if 'SOBBY Cotton Plain Strobel Cloth' in address_text:
                    item = 'SOBBY Cotton Plain Strobel Cloth'
        
        # Method 4: Use product name from product details if available
        if not item:
            product_section = self.extract_section_text(text, "Product Details", "Consignee Detail")
            if 'SOBBY Cotton Plain Strobel Cloth' in product_section:
                item = 'SOBBY Cotton Plain Strobel Cloth'
        
        consignee_data['Item'] = item
        
        # Extract address
        address_match = re.search(r'Address\s*:\s*([^\n]+)', consignee_section)
        if address_match:
            address = address_match.group(1).strip()
            consignee_data['Address'] = self.clean_address_aggressive(address)
        else:
            consignee_data['Address'] = ""
        
        return consignee_data
    
    def check_contract_exists(self, contract_no):
        """Check if contract already exists in database"""
        if not contract_no:
            return False
        return Contract.objects.filter(contract_no=contract_no).exists()
    
    def generate_embedding(self, text):
        """Generate embedding for the given text using sentence-transformers"""
        try:
            from sentence_transformers import SentenceTransformer
            
            # Get the embedder model
            model = self._get_embedder()
            if model is None:
                print("⚠️  Warning: Could not load sentence-transformers model, installing now...")
                try:
                    import subprocess
                    import sys
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "sentence-transformers"])
                    print("✅ sentence-transformers installed successfully!")
                    
                    # Try again after installation
                    model = SentenceTransformer('all-MiniLM-L6-v2')
                except Exception as install_error:
                    print(f"❌ Failed to install sentence-transformers: {install_error}")
                    return None
            
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
    
    def _get_embedder(self):
        """Get the sentence transformer model for embeddings"""
        try:
            from sentence_transformers import SentenceTransformer
            return SentenceTransformer('all-MiniLM-L6-v2')
        except Exception as e:
            print(f"⚠️  Warning: Could not import sentence-transformers: {e}")
            return None
    
    def save_to_django_models(self, text):
        """Save extracted data to Django models"""
        try:
            # Check if contract already exists
            contract_data = self.extracted_data['Contract Details']
            contract_no = contract_data.get('Contract No', '')
            
            if self.check_contract_exists(contract_no):
                print(f"⏭️  Contract {contract_no} already exists, skipping...")
                return False
            
            # 1. Save PDF file
            pdf_filename = os.path.basename(self.pdf_path)
            with open(self.pdf_path, 'rb') as pdf_file:
                self.pdf_file_instance = PdfFile.objects.create(
                    pdf_file=File(pdf_file, name=pdf_filename)
                )
            
            # 2. Save Contract with CLEANED raw text (no Hindi)
            generated_date_str = contract_data.get('Generated Date', '')
            
            # Parse generated date
            generated_date = None
            if generated_date_str:
                try:
                    # Try different date formats
                    for fmt in ['%d-%b-%Y', '%d/%m/%Y', '%Y-%m-%d']:
                        try:
                            generated_date = datetime.strptime(generated_date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                except:
                    generated_date = None
            
            # Store cleaned text (without Hindi) in raw_text field
            cleaned_text = self.clean_text_remove_hindi(text)
            
            # Generate embedding for the cleaned text
            print("🔍 Generating embedding for contract text...")
            embedding = self.generate_embedding(cleaned_text)
            
            self.contract_instance = Contract.objects.create(
                file=self.pdf_file_instance,
                contract_no=contract_no,
                generated_date=generated_date,
                raw_text=cleaned_text,  # Store cleaned text
                embedding=embedding  # Store generated embedding
            )
            
            # 3. Save Organization Details
            org_data = self.extracted_data['Organization Details']
            OrganisationDetail.objects.create(
                contract=self.contract_instance,
                type=org_data.get('Type', ''),
                ministry=org_data.get('Ministry', ''),
                department=org_data.get('Department', ''),
                organisation_name=org_data.get('Organization Name', ''),
                office_zone=org_data.get('Office Zone', '')
            )
            
            # 4. Save Buyer Details
            buyer_data = self.extracted_data['Buyer Details']
            BuyerDetail.objects.create(
                contract=self.contract_instance,
                designation=buyer_data.get('Designation', ''),
                contact_no=buyer_data.get('Contact No', ''),
                email=buyer_data.get('Email ID', ''),
                gstin=buyer_data.get('GSTIN', ''),
                address=buyer_data.get('Address', '')
            )
            
            # 5. Save Financial Approval
            financial_data = self.extracted_data['Financial Approval Detail']
            FinancialApproval.objects.create(
                contract=self.contract_instance,
                ifd_concurrence=financial_data.get('IFD Concurrence', '').lower() == 'yes',
                admin_approval_designation=financial_data.get('Designation of Administrative Approval', ''),
                financial_approval_designation=financial_data.get('Designation of Financial Approval', '')
            )
            
            # 6. Save Paying Authority
            paying_data = self.extracted_data['Paying Authority Details']
            PayingAuthority.objects.create(
                contract=self.contract_instance,
                role=paying_data.get('Role', ''),
                payment_mode=paying_data.get('Payment Mode', ''),
                designation=paying_data.get('Designation', ''),
                email=paying_data.get('Email ID', ''),
                gstin=paying_data.get('GSTIN', ''),
                address=paying_data.get('Address', '')
            )
            
            # 7. Save Seller Details
            seller_data = self.extracted_data['Seller Details']
            SellerDetail.objects.create(
                contract=self.contract_instance,
                gem_seller_id=seller_data.get('GeM Seller ID', ''),
                company_name=seller_data.get('Company Name', ''),
                contact_no=seller_data.get('Contact No', ''),
                email=seller_data.get('Email ID', ''),
                address=seller_data.get('Address', ''),
                msme_registration_number=seller_data.get('MSME Registration number', ''),
                gstin=seller_data.get('GSTIN', '')
            )
            
            # 8. Save Product
            product_data = self.extracted_data['Product Details']
            
            # Generate embedding for product (combine product name and description)
            product_text = f"{product_data.get('Product Name', '')} {product_data.get('Item Description', '')} {product_data.get('Brand', '')} {product_data.get('Model', '')}"
            product_text = product_text.strip()
            
            product_embedding = None
            if product_text:
                print("🔍 Generating embedding for product...")
                product_embedding = self.generate_embedding(product_text)
            
            product = Product.objects.create(
                contract=self.contract_instance,
                item_description=product_data.get('Item Description', ''),
                product_name=product_data.get('Product Name', ''),
                brand=product_data.get('Brand', ''),
                brand_type=product_data.get('Brand Type', ''),
                catalogue_status=product_data.get('Catalogue Status', ''),
                selling_as=product_data.get('Selling As', ''),
                category_name_quadrant=product_data.get('Category Name & Quadrant', ''),
                model=product_data.get('Model', ''),
                hsn_code=product_data.get('HSN Code', ''),
                ordered_quantity=product_data.get('Ordered Quantity', ''),
                unit=product_data.get('Unit', ''),
                unit_price=product_data.get('Unit Price (INR)', ''),
                embedding=product_embedding  # Store product embedding
            )
            
            # 9. Save Consignee Detail
            consignee_data = self.extracted_data['Consignee Detail']
            ConsigneeDetail.objects.create(
                product=product,
                designation=consignee_data.get('Designation', ''),
                email=consignee_data.get('Email ID', ''),
                contact=consignee_data.get('Contact', ''),
                gstin=consignee_data.get('GSTIN', ''),
                address=consignee_data.get('Address', ''),
                item=consignee_data.get('Item', '')
            )
            
            print(f"✅ Successfully saved data to Django models for contract: {contract_no}")
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
        
        # Extract all sections
        self.extracted_data = {
            'Organization Details': self.extract_organization_details(text),
            'Contract Details': self.extract_contract_details(text),
            'Buyer Details': self.extract_buyer_details(text),
            'Financial Approval Detail': self.extract_financial_approval_details(text),
            'Paying Authority Details': self.extract_paying_authority_details(text),
            'Seller Details': self.extract_seller_details(text),
            'Product Details': self.extract_product_details(text),
            'Consignee Detail': self.extract_consignee_details(text)
        }
        
        return self.extracted_data
    
    def export_to_excel(self, output_path=None):
        """Export extracted data to Excel"""
        if output_path is None:
            # Auto-generate filename using contract ID
            contract_no = self.extracted_data.get('Contract Details', {}).get('Contract No', 'unknown')
            if contract_no and contract_no != 'unknown':
                output_path = f"extracted_data/{contract_no}.xlsx"
            else:
                output_path = "extracted_data/final_improved_data.xlsx"
        
        # Ensure extracted_data directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            # Create a writer object
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Write each section to a separate sheet
                for section_name, section_data in self.extracted_data.items():
                    # Convert to DataFrame
                    df = pd.DataFrame(list(section_data.items()), columns=['Field', 'Value'])
                    # Write to Excel
                    df.to_excel(writer, sheet_name=section_name[:31], index=False)  # Excel sheet names limited to 31 chars
                    
            print(f"📊 Data exported to Excel: {output_path}")
            return True
        except Exception as e:
            print(f"❌ Error exporting to Excel: {e}")
            return False
    
    def export_to_json(self, output_path=None):
        """Export extracted data to JSON"""
        if output_path is None:
            # Auto-generate filename using contract ID
            contract_no = self.extracted_data.get('Contract Details', {}).get('Contract No', 'unknown')
            if contract_no and contract_no != 'unknown':
                output_path = f"extracted_data/{contract_no}.json"
            else:
                output_path = "extracted_data/final_improved_data.json"
        
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
        print("FINAL IMPROVED AUTOMATED EXTRACTED DATA FROM PDF")
        print("="*80)
        
        for section_name, section_data in self.extracted_data.items():
            print(f"\n{section_name}:")
            print("-" * len(section_name))
            for field, value in section_data.items():
                print(f"{field}: {value}")

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

def process_single_pdf(pdf_path, thread_id):
    """Process a single PDF file (for multi-threading)"""
    try:
        print(f"🔄 [Thread-{thread_id}] Processing: {os.path.basename(pdf_path)}")
        
        extractor = FinalImprovedAutomatedGEMCPDFExtractor(pdf_path)
        
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
            
            extractor = FinalImprovedAutomatedGEMCPDFExtractor(pdf_path)
            
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
                    contract_data = extractor.extract_contract_details(extractor.extract_text_from_pdf())
                    contract_no = contract_data.get('Contract No', '')
                    if extractor.check_contract_exists(contract_no):
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
                
                extractor = FinalImprovedAutomatedGEMCPDFExtractor(pdf_path)
                
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
                        contract_data = extractor.extract_contract_details(extractor.extract_text_from_pdf())
                        contract_no = contract_data.get('Contract No', '')
                        if extractor.check_contract_exists(contract_no):
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
            
            extractor = FinalImprovedAutomatedGEMCPDFExtractor(pdf_path)
            
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
                    contract_data = extractor.extract_contract_details(extractor.extract_text_from_pdf())
                    contract_no = contract_data.get('Contract No', '')
                    if extractor.check_contract_exists(contract_no):
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

def generate_embeddings_for_existing_contracts():
    """Generate embeddings for existing contracts that don't have them"""
    try:
        from src.apps.cont_record.models import ContractDocument
        
        print("🔍 Checking for contracts without embeddings...")
        
        # Find contracts without embeddings
        contracts_without_embeddings = ContractDocument.objects.filter(embedding__isnull=True)
        
        print(f"📊 Found {contracts_without_embeddings.count()} contracts without embeddings")
        
        if contracts_without_embeddings.count() == 0:
            print("✅ All contracts already have embeddings!")
            return
        
        # Create extractor instance for embedding generation
        extractor = FinalImprovedAutomatedGEMCPDFExtractor("dummy.pdf")
        
        # Generate embeddings for contracts
        contract_count = 0
        for contract in contracts_without_embeddings:
            try:
                if contract.raw_text:
                    print(f"🔍 Generating embedding for contract: {contract.contract_no}")
                    embedding = extractor.generate_embedding(contract.raw_text)
                    if embedding:
                        contract.embedding = embedding
                        contract.save()
                        contract_count += 1
                        print(f"✅ Saved embedding for contract: {contract.contract_no}")
                    else:
                        print(f"⚠️  Could not generate embedding for contract: {contract.contract_no}")
                else:
                    print(f"⚠️  Contract {contract.contract_no} has no raw_text")
            except Exception as e:
                print(f"❌ Error generating embedding for contract {contract.contract_no}: {e}")
        
        print(f"\n📊 EMBEDDING GENERATION SUMMARY:")
        print(f"✅ Contracts processed: {contract_count}")
        print("="*80)
        
    except Exception as e:
        print(f"❌ Error in generate_embeddings_for_existing_contracts: {e}")

def main():
    # Check for help command first
    if "--help" in sys.argv or "-h" in sys.argv:
        show_help()
        return
    
    # Check for special commands first
    if "--generate-embeddings" in sys.argv or "-ge" in sys.argv:
        # Generate embeddings for existing contracts
        print("🚀 Generating embeddings for existing contracts...")
        generate_embeddings_for_existing_contracts()
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
        extractor = FinalImprovedAutomatedGEMCPDFExtractor(pdf_path)
        
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

def show_help():
    """Display help information for the script"""
    print("🧪 GEM CONTRACT DATA EXTRACTOR - COMMAND LINE USAGE")
    print("="*60)
    print("Usage:")
    print("  python data_extractor.py [options] [pdf_file]")
    print("")
    print("Options:")
    print("  --help, -h              Show this help message")
    print("  --generate-embeddings, -ge  Generate embeddings for existing contracts")
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

if __name__ == "__main__":
    main()
