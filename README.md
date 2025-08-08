# PDF Data Extraction Project

A Django-based web application for extracting structured data from PDF documents and converting it to clean JSON format.

## Features

- PDF text extraction with OCR support for scanned documents
- Table detection and extraction
- Bilingual text handling (Hindi/English)
- Clean JSON output with structured data
- Web interface for PDF upload and processing
- Support for various PDF formats including scanned documents

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd pdf_data
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
# On Windows
venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Install Tesseract OCR (required for scanned PDFs):
   - **Windows**: Download and install from https://github.com/UB-Mannheim/tesseract/wiki
   - **macOS**: `brew install tesseract`
   - **Linux**: `sudo apt-get install tesseract-ocr`

5. Run migrations:
```bash
python manage.py migrate
```

6. Start the development server:
```bash
python manage.py runserver
```

7. Open your browser and navigate to `http://127.0.0.1:8000/`

## Usage

1. Upload a PDF file through the web interface
2. The system will extract text and tables from the PDF
3. View the extracted English text and parsed JSON data
4. The JSON output includes structured data for:
   - Contract details
   - Buyer information
   - Financial approval
   - Seller details
   - Product specifications
   - Terms and conditions
   - Organization details

## Project Structure

```
pdf_data/
├── src/
│   ├── apps/
│   │   └── cont_record/
│   │       ├── models.py
│   │       ├── views.py
│   │       ├── urls.py
│   │       └── admin.py
│   └── utils/
│       ├── extract_text.py
│       └── contract_parsers.py
├── templates/
│   └── contracts/
│       └── upload_pdfs.html
├── requirements.txt
├── manage.py
└── README.md
```

## Dependencies

- **Django**: Web framework
- **pdfplumber**: PDF text and table extraction
- **PyPDF2**: PDF processing
- **pytesseract**: OCR for scanned PDFs
- **Pillow**: Image processing
- **opencv-python**: Computer vision for image processing
- **numpy**: Numerical computing

## Features

- **Text Extraction**: Extracts text from both digital and scanned PDFs
- **Table Detection**: Identifies and extracts tabular data
- **Language Filtering**: Separates English text from bilingual content
- **Data Parsing**: Converts unstructured text to structured JSON
- **Web Interface**: User-friendly upload and processing interface

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License.
