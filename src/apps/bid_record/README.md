# Bid Data Extractor

A comprehensive PDF extraction system for bid documents, similar to the contract extractor but specifically designed for bid data.

## 🚀 Features

### ✅ **Core Functionality**
- **PDF Text Extraction**: Uses PyMuPDF for high-quality text extraction
- **Data Parsing**: Extracts structured data from bid documents
- **Django Integration**: Automatically saves data to Django models
- **Embedding Generation**: Creates semantic embeddings for AI-powered search
- **Duplicate Prevention**: Skips already processed bids

### ✅ **Data Extraction Fields**
- **Bid Details**: Bid Number, Date, Source File
- **Organization**: Ministry, Department, Organization Name, Office Name
- **Contact**: Buyer Email, Beneficiary, Delivery Address
- **Bid Info**: Estimated Value, Quantity, Contract Period, Delivery Days
- **Product**: Item Category, Scope of Supply, Option Clause
- **Evaluation**: Method, Inspection Required, MII/MSE Preferences
- **Timing**: Bid End/Open Dates, Validity Period, Technical Clarification Time

### ✅ **Advanced Features**
- **Multi-threading**: Process multiple PDFs concurrently
- **Recursive Scanning**: Find PDFs in subdirectories
- **Export Options**: Excel and JSON export
- **Text Cleaning**: Remove Hindi characters and artifacts
- **Error Handling**: Graceful failure handling with detailed logging

## 📁 File Structure

```
src/apps/bid_record/
├── utils/
│   ├── text_extractor.py      # Main extraction logic
│   ├── serialization.py       # Data serialization helpers
│   └── test_extractor.py      # Test script
├── models.py                  # Django models
├── views.py                   # Django views (including table view)
├── urls.py                    # URL routing
└── README.md                  # This file
```

## 🛠️ Installation & Setup

### 1. **Install Dependencies**
```bash
pip install sentence-transformers PyMuPDF pandas openpyxl
```

### 2. **Django Setup**
The extractor automatically sets up Django environment when imported.

### 3. **Create Data Directory**
```bash
mkdir -p src/apps/bid_record/data
mkdir -p src/apps/bid_record/extracted_data
```

## 🎯 Usage Examples

### **Single PDF Processing**
```python
from utils.text_extractor import FinalImprovedAutomatedBidPDFExtractor

# Process a single PDF
extractor = FinalImprovedAutomatedBidPDFExtractor("path/to/bid.pdf")
data = extractor.extract_all_data()

if data:
    # Save to Django models
    text = extractor.extract_text_from_pdf()
    extractor.save_to_django_models(text)
    
    # Export to files
    extractor.export_to_excel()
    extractor.export_to_json()
```

### **Command Line Usage**

#### **Process Single PDF**
```bash
cd src/apps/bid_record/utils
python text_extractor.py path/to/bid.pdf
```

#### **Process All PDFs (Single-threaded)**
```bash
cd src/apps/bid_record/utils
python text_extractor.py
```

#### **Process All PDFs (Multi-threaded)**
```bash
cd src/apps/bid_record/utils
python text_extractor.py --multi-thread --workers=4
```

#### **Generate Embeddings for Existing Data**
```bash
cd src/apps/bid_record/utils
python text_extractor.py --generate-embeddings
```

### **Batch Processing Functions**

#### **Single-threaded**
```python
from utils.text_extractor import process_all_pdfs_in_data_directory

# Process all PDFs in data directory
process_all_pdfs_in_data_directory()
```

#### **Multi-threaded**
```python
from utils.text_extractor import process_all_pdfs_in_data_directory_multi_threaded

# Process with 4 workers
process_all_pdfs_in_data_directory_multi_threaded(max_workers=4)
```

## 🌐 Web Interface

### **Bid Data Table View**
- **URL**: `/bid/view/`
- **Features**: 
  - Search and filter bids
  - Pagination
  - Export to Excel/CSV
  - View detailed bid information
  - Same design as contract table

### **Navigation**
The "Bid Data" link has been added to the main header navigation, positioned after "View Data".

## 📊 Data Model

### **BidDocument Model**
```python
class BidDocument(models.Model):
    # Core fields
    file = models.FileField(upload_to='bids/')
    dated = models.DateField(null=True, blank=True)
    bid_number = models.CharField(max_length=100, null=True, blank=True)
    
    # Organization details
    ministry = models.CharField(max_length=255, null=True, blank=True)
    department = models.CharField(max_length=255, null=True, blank=True)
    organisation = models.CharField(max_length=255, null=True, blank=True)
    office_name = models.CharField(max_length=255, null=True, blank=True)
    
    # Contact details
    buyer_email = models.EmailField(null=True, blank=True)
    beneficiary = models.CharField(max_length=255, null=True, blank=True)
    delivery_address = models.TextField(null=True, blank=True)
    
    # Bid information
    estimated_bid_value = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    total_quantity = models.IntegerField(null=True, blank=True)
    contract_period = models.CharField(max_length=255, null=True, blank=True)
    item_category = models.TextField(null=True, blank=True)
    
    # Additional fields
    bid_end_datetime = models.CharField(max_length=100, null=True, blank=True)
    bid_open_datetime = models.CharField(max_length=100, null=True, blank=True)
    bid_offer_validity_days = models.IntegerField(null=True, blank=True)
    primary_product_category = models.CharField(max_length=255, null=True, blank=True)
    technical_clarification_time = models.CharField(max_length=100, null=True, blank=True)
    inspection_required = models.CharField(max_length=10, null=True, blank=True)
    evaluation_method = models.CharField(max_length=255, null=True, blank=True)
    mii_purchase_preference = models.CharField(max_length=10, null=True, blank=True)
    mse_purchase_preference = models.CharField(max_length=10, null=True, blank=True)
    delivery_days = models.IntegerField(null=True, blank=True)
    scope_of_supply = models.TextField(null=True, blank=True)
    option_clause = models.TextField(null=True, blank=True)
    
    # Text and embeddings
    raw_text = models.TextField(null=True, blank=True)
    embedding = models.JSONField(null=True, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
```

## 🔍 Text Extraction Patterns

### **Bid Number**
- Pattern: `Bid Number[:\s]+([A-Z0-9\/\-]+)`
- Fallback: `GEMC-\d+`

### **Date Extraction**
- Formats: `dd-mm-yyyy`, `dd/mm/yyyy`, `yyyy-mm-dd`, `Month dd, yyyy`
- Pattern: `Dated[:\s]+([^\n]+)`

### **Organization Details**
- Ministry: `Ministry[^\n]*\n?([A-Za-z &]+)`
- Department: `Department\s+Name\s*([A-Za-z &]+)`
- Organization: `Organisation\s+Name\s*([A-Za-z &\(\)]+)`
- Office: `Office\s+Name\s*([A-Za-z0-9* &]+)`

### **Bid Information**
- End Date: `Bid\s+End\s+Date/Time\s+([0-9]{2}-[0-9]{2}-[0-9]{4} [0-9:]+)`
- Estimated Value: `Estimated\s+Bid\s+Value\s*[:\s]*([^\n]+)`
- Quantity: `Total\s+Quantity\s+(\d+)`
- Contract Period: `Contract\s+Period\s*([^\n]+)`

## 🧪 Testing

### **Run Test Suite**
```bash
cd src/apps/bid_record
python utils/test_extractor.py
```

### **Test Options**
1. **Single PDF Processing**: Test extraction from one PDF
2. **Batch Processing**: Test processing multiple PDFs
3. **Embedding Generation**: Test embedding creation
4. **All Tests**: Run complete test suite

## 📝 Configuration

### **Embedding Model**
- **Model**: `all-MiniLM-L6-v2`
- **Dimensions**: 384
- **Normalization**: L2 normalized

### **Text Cleaning**
- Removes Hindi characters (`[^\x00-\x7F]+`)
- Removes specific mixed text patterns
- Normalizes whitespace
- Removes PDF artifacts

### **Directory Structure**
```
src/apps/bid_record/
├── data/                    # Place PDFs here
├── extracted_data/          # Exported files
└── utils/
    └── text_extractor.py    # Main script
```

## 🚨 Error Handling

### **Common Issues**
1. **PDF Not Found**: Ensure PDF exists in specified path
2. **Django Setup**: Ensure Django environment is properly configured
3. **Dependencies**: Install required packages (`sentence-transformers`, `PyMuPDF`)
4. **Permissions**: Ensure write access to output directories

### **Logging**
- ✅ Success messages with green checkmarks
- ⏭️ Skipped items (duplicates)
- ❌ Error messages with details
- 🔍 Processing status updates

## 🔄 Workflow

### **Typical Usage Flow**
1. **Place PDFs** in `data/` directory
2. **Run extractor** (single or batch mode)
3. **Data extracted** and saved to Django models
4. **Embeddings generated** for semantic search
5. **Files exported** to Excel/JSON format
6. **View data** in web interface at `/bid/view/`

### **Data Processing Pipeline**
```
PDF Input → Text Extraction → Data Parsing → Django Models → Embeddings → Export
```

## 📈 Performance

### **Multi-threading Benefits**
- **4 workers**: ~4x faster than single-threaded
- **Memory efficient**: Processes PDFs in batches
- **Scalable**: Adjust worker count based on system resources

### **Optimization Tips**
- Use multi-threading for large datasets
- Place PDFs in organized subdirectories
- Monitor memory usage with very large PDFs
- Use SSD storage for better I/O performance

## 🤝 Contributing

### **Adding New Fields**
1. Update the `BidDocument` model
2. Add extraction logic in `extract_*` methods
3. Update the table view template
4. Test with sample data

### **Improving Patterns**
1. Analyze failed extractions
2. Add new regex patterns
3. Test with diverse PDF formats
4. Update documentation

## 📞 Support

For issues or questions:
1. Check the error logs
2. Verify PDF format compatibility
3. Ensure all dependencies are installed
4. Test with the provided test suite

---

**🎯 The Bid Data Extractor provides the same powerful functionality as the Contract Extractor, but specifically designed for bid documents with comprehensive field extraction and modern web interface.**
