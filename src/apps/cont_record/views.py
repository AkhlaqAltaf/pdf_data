# contracts/views.py
from django.views.generic import TemplateView
from django.shortcuts import render
from django.contrib import messages
import json
from django.core.files.storage import FileSystemStorage

from src.utils.contract_parsers import parse_contract_text_to_json
from src.utils.extract_text import read_pdf_with_structure, _extract_english_from_pdf


def upload_pdfs(request):
    if request.method == 'POST':
        uploaded_file = request.FILES.get('pdf_file')
        
        if uploaded_file and uploaded_file.name.endswith('.pdf'):
            # Save the uploaded file temporarily
            fs = FileSystemStorage()
            filename = fs.save(uploaded_file.name, uploaded_file)
            file_path = fs.path(filename)
            
            try:
                # Extract text and tables from PDF
                extraction_result = read_pdf_with_structure(file_path)
                raw_text = extraction_result['text']
                tables = extraction_result.get('tables', [])
                
                # Extract English-only text
                english_text = _extract_english_from_pdf(raw_text)
                
                # Parse the English text to JSON
                parsed_data = parse_contract_text_to_json(english_text, tables)
                
                # Prepare context for template
                context = {
                    'uploaded_file': uploaded_file,
                    'extraction_result': extraction_result,
                    'parsed_data': json.dumps(parsed_data, indent=2, ensure_ascii=False),
                    'english_text': english_text,  # Show FULL English text
                    'english_text_length': len(english_text),
                    'summary': {
                        'pages': extraction_result.get('pages_count', 0),
                        'tables_count': len(tables),
                        'english_text_length': len(english_text),
                        'extraction_method': extraction_result.get('method', 'unknown'),
                        'ocr_used': extraction_result.get('ocr_used', False)
                    }
                }
                
                # Clean up the temporary file
                fs.delete(filename)
                
                return render(request, 'contracts/upload_pdfs.html', context)
                
            except Exception as e:
                # Clean up the temporary file in case of error
                fs.delete(filename)
                context = {
                    'error': f'Error processing PDF: {str(e)}'
                }
                return render(request, 'contracts/upload_pdfs.html', context)
        else:
            context = {
                'error': 'Please upload a valid PDF file.'
            }
            return render(request, 'contracts/upload_pdfs.html', context)
    
    return render(request, 'contracts/upload_pdfs.html')
