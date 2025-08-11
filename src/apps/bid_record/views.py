import os
from django.shortcuts import render, redirect
from django.conf import settings
from .models import BidDocument
from django.core.files.storage import FileSystemStorage

from .utils.serialization import make_serializable
from .utils.text_extractor import extract_bid_info_from_pdf


def upload_bid_document(request):
    context = {}
    if request.method == "POST":
        if "upload_file" in request.POST:
            uploaded_file = request.FILES.get("file")
            if uploaded_file:
                fs = FileSystemStorage(location=os.path.join(settings.MEDIA_ROOT, 'temp'))
                filename = fs.save(uploaded_file.name, uploaded_file)
                file_path = fs.path(filename)

                extracted_data = extract_bid_info_from_pdf(file_path)
                request.session["extracted_data"] = make_serializable(extracted_data)
                request.session["uploaded_file_path"] = file_path
                context["data"] = extracted_data
                context["preview"] = True

        elif "save_data" in request.POST:
            extracted_data = request.session.get("extracted_data")
            file_path = request.session.get("uploaded_file_path")
            if extracted_data and file_path:
                bid = BidDocument.objects.create(
                    file=os.path.join('bids', os.path.basename(file_path)),
                    **{k: v for k, v in extracted_data.items() if k in [f.name for f in BidDocument._meta.get_fields()]}
                )
                return redirect("bid_record:upload_bid_document")

    return render(request, "bid/upload_bid.html", context)
