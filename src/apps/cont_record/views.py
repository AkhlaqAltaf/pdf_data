# contracts/views.py
from django.views.generic import TemplateView
from django.shortcuts import render
from django.contrib import messages

from src.utils.contract_parsers import parse_contract_text_to_json
from src.utils.extract_text import read_pdf_text


class PDFUploadView(TemplateView):
    template_name = "contracts/upload_pdfs.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        # Provide defaults
        ctx.setdefault("results", kwargs.get("results", []))
        ctx.setdefault("uploaded", kwargs.get("uploaded", False))
        return ctx

    def post(self, request, *args, **kwargs):
        files = request.FILES.getlist("pdfs")
        results = []

        if not files:
            messages.warning(request, "Please select one or more PDF files to upload.")
            return self.get(request, *args, **kwargs)

        for uploaded in files:
            entry = {"filename": uploaded.name}
            # simple validation by extension/content-type
            if not uploaded.name.lower().endswith(".pdf") and uploaded.content_type != "application/pdf":
                entry["error"] = "Not a PDF (filename/content-type mismatch)."
                results.append(entry)
                continue

            try:
                # Call the util function (it accepts UploadedFile)
                extracted_text = read_pdf_text(uploaded)
                print(extracted_text)

                # If empty, note that
                if not extracted_text:
                    entry["text"] = ""
                    entry["notice"] = "No text extracted (PDF may be scanned image)."
                else:

                    parsed = parse_contract_text_to_json(extracted_text)
                    import json
                    print(json.dumps(parsed, indent=2, ensure_ascii=False))
                    data = json.dumps(parsed, indent=2, ensure_ascii=False)

                    entry["text"] = data
            except Exception as e:
                entry["error"] = f"Failed to read PDF: {e!s}"

            results.append(entry)

        context = self.get_context_data(results=results, uploaded=True)
        return self.render_to_response(context)
