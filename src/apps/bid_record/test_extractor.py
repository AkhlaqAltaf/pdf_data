#!/usr/bin/env python3
"""
Test script for the Bid Data Extractor
This script demonstrates how to use the bid data extractor
"""

import os
import sys
import django
from pathlib import Path

# Setup Django environment
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pdf_data.settings')
django.setup()

from utils.text_extractor import FinalImprovedAutomatedBidPDFExtractor

def test_single_pdf():
    """Test processing a single PDF file"""
    # Example usage - replace with actual PDF path
    pdf_path = "data/sample_bid.pdf"
    
    if not os.path.exists(pdf_path):
        print(f"❌ PDF file not found: {pdf_path}")
        print("Please place a PDF file in the data directory and update the path")
        return
    
    print(f"🔍 Testing bid data extraction for: {pdf_path}")
    print("="*60)
    
    # Create extractor instance
    extractor = FinalImprovedAutomatedBidPDFExtractor(pdf_path)
    
    # Extract data
    data = extractor.extract_all_data()
    
    if data:
        print("✅ Data extraction successful!")
        print("\n📊 Extracted Data:")
        print("-" * 40)
        
        for section_name, section_data in data.items():
            print(f"\n{section_name}:")
            for field, value in section_data.items():
                print(f"  {field}: {value}")
        
        # Test saving to Django models
        print("\n💾 Testing Django model save...")
        text = extractor.extract_text_from_pdf()
        if extractor.save_to_django_models(text):
            print("✅ Successfully saved to Django models!")
        else:
            print("⏭️  Bid already exists in database")
        
        # Test export
        print("\n📤 Testing export functionality...")
        extractor.export_to_excel()
        extractor.export_to_json()
        
    else:
        print("❌ Data extraction failed!")

def test_batch_processing():
    """Test batch processing of PDFs"""
    print("🚀 Testing batch processing...")
    print("="*60)
    
    # Test single-threaded processing
    print("📁 Testing single-threaded processing...")
    from utils.text_extractor import process_all_pdfs_in_data_directory
    process_all_pdfs_in_data_directory()
    
    # Test multi-threaded processing
    print("\n🚀 Testing multi-threaded processing...")
    from utils.text_extractor import process_all_pdfs_in_data_directory_multi_threaded
    process_all_pdfs_in_data_directory_multi_threaded(max_workers=2)

def test_embedding_generation():
    """Test embedding generation for existing bids"""
    print("🔍 Testing embedding generation...")
    print("="*60)
    
    from utils.text_extractor import generate_embeddings_for_existing_bids
    generate_embeddings_for_existing_bids()

if __name__ == "__main__":
    print("🧪 BID DATA EXTRACTOR TEST SUITE")
    print("="*60)
    
    # Check if data directory exists
    data_dir = Path(__file__).parent.parent.parent / "data"
    if not data_dir.exists():
        print("📁 Creating data directory...")
        data_dir.mkdir(exist_ok=True)
        print("✅ Data directory created. Please place PDF files in this directory.")
    
    print("\nChoose a test to run:")
    print("1. Test single PDF processing")
    print("2. Test batch processing")
    print("3. Test embedding generation")
    print("4. Run all tests")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == "1":
        test_single_pdf()
    elif choice == "2":
        test_batch_processing()
    elif choice == "3":
        test_embedding_generation()
    elif choice == "4":
        test_single_pdf()
        print("\n" + "="*60)
        test_batch_processing()
        print("\n" + "="*60)
        test_embedding_generation()
    else:
        print("❌ Invalid choice. Please run the script again.")
    
    print("\n✅ Test suite completed!")
