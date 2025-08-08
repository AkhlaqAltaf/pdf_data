# contracts/utils.py
import pathlib
from io import BytesIO
from typing import Union

# Try to import the preferred pdf libraries
try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    from PyPDF2 import PdfReader
except Exception:
    PdfReader = None


def read_pdf_text(pdf: Union[str, pathlib.Path, object]) -> str:
    """
    Read a PDF and return a simple concatenated plain-text string.

    Accepts:
      - path string or pathlib.Path -> opens file from disk
      - Django UploadedFile (InMemoryUploadedFile or TemporaryUploadedFile)
      - any file-like object with .read()

    Uses pdfplumber if available (preferred), otherwise PyPDF2.
    Raises ImportError if neither library is available.
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

    text_chunks = []

    try:

        if pdfplumber is not None:
            # pdfplumber accepts file-like objects
            with pdfplumber.open(stream) as doc:
                for page in doc.pages:
                    t = page.extract_text()
                    if t:
                        text_chunks.append(t)
        elif PdfReader is not None:
            # PyPDF2 fallback
            reader = PdfReader(stream)
            for page in reader.pages:
                try:
                    t = page.extract_text()
                except Exception:
                    t = None
                if t:
                    text_chunks.append(t)
        else:
            raise ImportError("No PDF parsing library found. Install 'pdfplumber' or 'PyPDF2'.")
    finally:
        # Close any file we explicitly opened
        if opened_file:
            try:
                opened_file.close()
            except Exception:
                pass

    return "\n\n".join(text_chunks).strip()
