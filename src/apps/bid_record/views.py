import os
from django.shortcuts import render, redirect
from django.conf import settings
from .models import BidDocument
from django.core.files.storage import FileSystemStorage
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.db.models import Q
from django.http import JsonResponse, HttpResponse
from django.views import View
import pandas as pd
import json
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from .utils.serialization import make_serializable
from .data_extractor import GeMBiddingPDFExtractor


def extract_bid_info_from_pdf(pdf_path):
    """Extract bid information from PDF using the GeMBiddingPDFExtractor"""
    try:
        extractor = GeMBiddingPDFExtractor(pdf_path)
        extracted_data = extractor.extract_all_data()
        if extracted_data:
            # Add the raw text
            extracted_data['raw_text'] = extractor.extract_text_from_pdf()
            return extracted_data
        return {}
    except Exception as e:
        print(f"Error extracting bid info: {e}")
        return {}


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


class BidTableView(View):
    template_name = "bid/bid_table.html"
    per_page = 50

    def get(self, request):
        # Extract filters
        search_query = request.GET.get("search", "").strip()
        org_filter = request.GET.get("organisation", "").strip()
        dept_filter = request.GET.get("department", "").strip()
        ministry_filter = request.GET.get("ministry", "").strip()
        date_from = request.GET.get("date_from", "").strip()
        date_to = request.GET.get("date_to", "").strip()

        # Base queryset
        bids_qs = BidDocument.objects.all().order_by('-dated', '-created_at')

        # Apply filters
        if org_filter:
            bids_qs = bids_qs.filter(organisation__icontains=org_filter)
        if dept_filter:
            bids_qs = bids_qs.filter(department__icontains=dept_filter)
        if ministry_filter:
            bids_qs = bids_qs.filter(ministry__icontains=ministry_filter)
        if date_from:
            bids_qs = bids_qs.filter(dated__gte=date_from)
        if date_to:
            bids_qs = bids_qs.filter(dated__lte=date_to)
        if search_query:
            bids_qs = bids_qs.filter(
                # Bid basic info
                Q(bid_number__icontains=search_query) |
                Q(dated__icontains=search_query) |
                Q(source_file__icontains=search_query) |
                
                # Organisation details
                Q(organisation__icontains=search_query) |
                Q(department__icontains=search_query) |
                Q(ministry__icontains=search_query) |
                
                # Bid details
                Q(beneficiary__icontains=search_query) |
                Q(contract_period__icontains=search_query) |
                Q(item_category__icontains=search_query) |
                Q(similar_category__icontains=search_query) |
                Q(mse_exemption__icontains=search_query) |
                
                # Bid timing
                Q(bid_end_datetime__icontains=search_query) |
                Q(bid_open_datetime__icontains=search_query) |
                Q(bid_offer_validity_days__icontains=search_query) |
                
                # Raw text for comprehensive search
                Q(raw_text__icontains=search_query)
            ).distinct()

        # Build comprehensive data structure
        bid_data = []
        for bid in bids_qs:
            bid_data.append({
                # Core bid fields
                "bid_number": bid.bid_number,
                "dated": bid.dated,
                "source_file": bid.source_file,
                "raw_text": bid.raw_text,

                # Organization details
                "ministry": bid.ministry,
                "department": bid.department,
                "organisation": bid.organisation,

                # Bid details
                "beneficiary": bid.beneficiary,
                "contract_period": bid.contract_period,
                "item_category": bid.item_category,

                # Additional fields
                "bid_end_datetime": bid.bid_end_datetime,
                "bid_open_datetime": bid.bid_open_datetime,
                "bid_offer_validity_days": bid.bid_offer_validity_days,
                "similar_category": bid.similar_category,
                "mse_exemption": bid.mse_exemption,

                # Metadata
                "created_at": bid.created_at,
                "file": bid.file,
            })

        # Export handling
        if request.GET.get("export") in ["excel", "csv"]:
            return self.export_data(bid_data, request.GET.get("export"))

        # Pagination
        paginator = Paginator(bids_qs, self.per_page)
        page_number = request.GET.get('page', 1)
        page_obj = paginator.get_page(page_number)

        # Filter options
        org_options = BidDocument.objects.exclude(organisation="").values_list("organisation", flat=True).distinct()
        dept_options = BidDocument.objects.exclude(department="").values_list("department", flat=True).distinct()
        ministry_options = BidDocument.objects.exclude(ministry="").values_list("ministry", flat=True).distinct()

        return render(request, self.template_name, {
            "page_obj": page_obj,
            "search_query": search_query,
            "org_filter": org_filter,
            "dept_filter": dept_filter,
            "ministry_filter": ministry_filter,
            "date_from": date_from,
            "date_to": date_to,
            "org_options": org_options,
            "dept_options": dept_options,
            "ministry_options": ministry_options,
        })

    def export_data(self, rows, file_type):
        df = pd.DataFrame(rows)
        if "bid_obj" in df.columns:
            df = df.drop(columns=["bid_obj"])
        if file_type == "excel":
            response = HttpResponse(content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            response["Content-Disposition"] = 'attachment; filename="bids.xlsx"'
            df.to_excel(response, index=False)
            return response
        elif file_type == "csv":
            response = HttpResponse(content_type="text/csv")
            response["Content-Disposition"] = 'attachment; filename="bids.csv"'
            df.to_csv(response, index=False)
            return response


def get_bid_details_api(request, bid_id):
    """API endpoint to get bid details by ID"""
    try:
        bid = BidDocument.objects.get(id=bid_id)
        data = {
            'id': bid.id,
            'bid_number': bid.bid_number,
            'dated': bid.dated.isoformat() if bid.dated else None,
            'source_file': bid.source_file,
            'raw_text': bid.raw_text,
            'ministry': bid.ministry,
            'department': bid.department,
            'organisation': bid.organisation,
            'beneficiary': bid.beneficiary,
            'contract_period': bid.contract_period,
            'item_category': bid.item_category,
            'bid_end_datetime': bid.bid_end_datetime,
            'bid_open_datetime': bid.bid_open_datetime,
            'bid_offer_validity_days': bid.bid_offer_validity_days,
            'similar_category': bid.similar_category,
            'mse_exemption': bid.mse_exemption,
            'created_at': bid.created_at.isoformat() if bid.created_at else None,
            'file': bid.file.url if bid.file else None,
        }
        return JsonResponse(data)
    except BidDocument.DoesNotExist:
        return JsonResponse({'error': 'Bid not found'}, status=404)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


def ai_search_bids(request):
    """AI Semantic Search for Bids using embeddings"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Only POST method allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        query = data.get('query', '').strip()
        top_k = data.get('top_k', 5)
        
        if not query:
            return JsonResponse({'success': False, 'message': 'Query is required'}, status=400)
        
        # Get all bids with embeddings
        bids = BidDocument.objects.filter(embedding__isnull=False).exclude(embedding='')
        
        if not bids.exists():
            return JsonResponse({'success': False, 'message': 'No bids with embeddings found'}, status=404)
        
        # Compute query embedding
        try:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer('all-MiniLM-L6-v2')
            query_embedding = model.encode([query], normalize_embeddings=True)[0]
        except Exception as e:
            return JsonResponse({'success': False, 'message': f'Error computing query embedding: {str(e)}'}, status=500)
        
        # Calculate similarities
        results = []
        for bid in bids:
            try:
                bid_embedding = np.array(bid.embedding)
                similarity = cosine_similarity([query_embedding], [bid_embedding])[0][0]
                results.append({
                    'bid': bid,
                    'score': float(similarity)
                })
            except Exception as e:
                print(f"Error processing bid {bid.id}: {e}")
                continue
        
        # Sort by similarity score (descending)
        results.sort(key=lambda x: x['score'], reverse=True)
        
        # Take top_k results
        top_results = results[:top_k]
        
        # Format results for response
        formatted_results = []
        for result in top_results:
            bid = result['bid']
            formatted_results.append({
                'id': bid.id,
                'bid_number': bid.bid_number,
                'dated': bid.dated.isoformat() if bid.dated else None,
                'organisation': bid.organisation,
                'ministry': bid.ministry,
                'department': bid.department,
                'beneficiary': bid.beneficiary,
                'item_category': bid.item_category,
                'contract_period': bid.contract_period,
                'score': result['score']
            })
        
        # Generate summary
        if formatted_results:
            summary = f"Found {len(formatted_results)} relevant bids for '{query}'"
        else:
            summary = f"No relevant bids found for '{query}'"
        
        return JsonResponse({
            'success': True,
            'results': formatted_results,
            'summary': summary,
            'total_found': len(formatted_results)
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'message': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'Search error: {str(e)}'}, status=500)
