from django.core.management.base import BaseCommand
from django.db.models import Q
import pandas as pd
from datetime import datetime
import os

from src.apps.cont_record.models import (
    Contract, OrganisationDetail, BuyerDetail, SellerDetail, 
    Product, ConsigneeDetail, PayingAuthority, FinancialApproval
)


class Command(BaseCommand):
    help = "Filter contracts for army-related keywords and generate Excel file with complete data"

    def add_arguments(self, parser):
        parser.add_argument(
            '--output',
            type=str,
            default='army_contracts_filtered.xlsx',
            help='Output Excel file name (default: army_contracts_filtered.xlsx)'
        )
        parser.add_argument(
            '--keywords',
            type=str,
            default='India Army,HQ,Headquarters,Armd,ARMD,army,ARMY,Headquarters,headquarters',
            help='Comma-separated keywords to search for'
        )
        parser.add_argument(
            '--min-fields',
            type=int,
            default=5,
            help='Minimum number of required fields that must be present (default: 5)'
        )

    def handle(self, *args, **options):
        output_file = options['output']
        keywords = [kw.strip() for kw in options['keywords'].split(',')]
        min_fields = options['min_fields']

        self.stdout.write(self.style.NOTICE(f'Searching for contracts with keywords: {keywords}'))
        self.stdout.write(self.style.NOTICE(f'Minimum required fields: {min_fields}'))

        # Build search query
        search_query = Q()
        for keyword in keywords:
            search_query |= (
                Q(raw_text__icontains=keyword) |
                Q(contract_no__icontains=keyword) |
                Q(organization_details__organisation_name__icontains=keyword) |
                Q(organization_details__department__icontains=keyword) |
                Q(organization_details__ministry__icontains=keyword) |
                Q(buyer__address__icontains=keyword) |
                Q(seller__company_name__icontains=keyword) |
                Q(seller__address__icontains=keyword) |
                Q(products__item_description__icontains=keyword) |
                Q(products__product_name__icontains=keyword) |
                Q(products__consignees__address__icontains=keyword) |
                Q(products__consignees__delivery_to__icontains=keyword) |
                Q(paying_authority__address__icontains=keyword)
            )

        # Get contracts matching keywords
        contracts = Contract.objects.filter(search_query).distinct().select_related(
            'organization_details', 'buyer', 'seller', 'paying_authority', 'financial_approval'
        ).prefetch_related(
            'products', 'products__consignees', 'products__specifications'
        )

        self.stdout.write(self.style.NOTICE(f'Found {contracts.count()} contracts matching keywords'))

        # Filter contracts with complete data
        filtered_data = []
        required_fields = [
            'contract_no', 'generated_date', 'raw_text',
            'organization_details__organisation_name', 'organization_details__department',
            'buyer__designation', 'buyer__address', 'buyer__contact_no',
            'seller__company_name', 'seller__address', 'seller__contact_no',
            'paying_authority__designation', 'paying_authority__address'
        ]

        for contract in contracts:
            # Check if contract has minimum required fields
            field_count = 0
            contract_dict = {
                'Contract No': contract.contract_no or '',
                'Generated Date': contract.generated_date.strftime('%Y-%m-%d') if contract.generated_date else '',
                'Raw Text': contract.raw_text[:500] + '...' if contract.raw_text and len(contract.raw_text) > 500 else contract.raw_text or '',
            }

            # Organization Details
            org_details = getattr(contract, 'organization_details', None)
            if org_details:
                contract_dict.update({
                    'Organization Type': org_details.type or '',
                    'Ministry': org_details.ministry or '',
                    'Department': org_details.department or '',
                    'Organization Name': org_details.organisation_name or '',
                    'Office Zone': org_details.office_zone or '',
                })
                field_count += sum(1 for v in [org_details.type, org_details.ministry, org_details.department, 
                                              org_details.organisation_name, org_details.office_zone] if v)

            # Buyer Details
            buyer = getattr(contract, 'buyer', None)
            if buyer:
                contract_dict.update({
                    'Buyer Designation': buyer.designation or '',
                    'Buyer Contact': buyer.contact_no or '',
                    'Buyer Email': buyer.email or '',
                    'Buyer GSTIN': buyer.gstin or '',
                    'Buyer Address': buyer.address or '',
                })
                field_count += sum(1 for v in [buyer.designation, buyer.contact_no, buyer.email, 
                                              buyer.gstin, buyer.address] if v)

            # Seller Details
            seller = getattr(contract, 'seller', None)
            if seller:
                contract_dict.update({
                    'Seller GEM ID': seller.gem_seller_id or '',
                    'Seller Company': seller.company_name or '',
                    'Seller Contact': seller.contact_no or '',
                    'Seller Email': seller.email or '',
                    'Seller Address': seller.address or '',
                    'Seller MSME': seller.msme_registration_number or '',
                    'Seller GSTIN': seller.gstin or '',
                })
                field_count += sum(1 for v in [seller.gem_seller_id, seller.company_name, seller.contact_no,
                                              seller.email, seller.address, seller.msme_registration_number, 
                                              seller.gstin] if v)

            # Paying Authority
            paying_auth = getattr(contract, 'paying_authority', None)
            if paying_auth:
                contract_dict.update({
                    'PA Role': paying_auth.role or '',
                    'PA Payment Mode': paying_auth.payment_mode or '',
                    'PA Designation': paying_auth.designation or '',
                    'PA Email': paying_auth.email or '',
                    'PA GSTIN': paying_auth.gstin or '',
                    'PA Address': paying_auth.address or '',
                })
                field_count += sum(1 for v in [paying_auth.role, paying_auth.payment_mode, paying_auth.designation,
                                              paying_auth.email, paying_auth.gstin, paying_auth.address] if v)

            # Financial Approval
            fin_approval = getattr(contract, 'financial_approval', None)
            if fin_approval:
                contract_dict.update({
                    'IFD Concurrence': 'Yes' if fin_approval.ifd_concurrence else 'No',
                    'Admin Approval Designation': fin_approval.admin_approval_designation or '',
                    'Financial Approval Designation': fin_approval.financial_approval_designation or '',
                })
                field_count += sum(1 for v in [fin_approval.ifd_concurrence, fin_approval.admin_approval_designation,
                                              fin_approval.financial_approval_designation] if v)

            # Products
            products = list(contract.products.all())
            if products:
                product_info = []
                for i, product in enumerate(products[:3]):  # Limit to first 3 products
                    product_data = {
                        f'Product {i+1} Name': product.product_name or '',
                        f'Product {i+1} Brand': product.brand or '',
                        f'Product {i+1} Model': product.model or '',
                        f'Product {i+1} HSN': product.hsn_code or '',
                        f'Product {i+1} Quantity': product.ordered_quantity or '',
                        f'Product {i+1} Unit': product.unit or '',
                        f'Product {i+1} Unit Price': product.unit_price or '',
                        f'Product {i+1} Total Price': product.total_price or '',
                        f'Product {i+1} Description': product.item_description[:200] + '...' if product.item_description and len(product.item_description) > 200 else product.item_description or '',
                    }
                    product_info.append(product_data)
                    field_count += sum(1 for v in product_data.values() if v)

                # Add product info to contract dict
                for product_data in product_info:
                    contract_dict.update(product_data)

            # Check if contract meets minimum field requirement
            if field_count >= min_fields:
                filtered_data.append(contract_dict)
                self.stdout.write(f'✓ Contract {contract.contract_no} - {field_count} fields')
            else:
                self.stdout.write(f'✗ Contract {contract.contract_no} - {field_count} fields (below threshold)')

        self.stdout.write(self.style.SUCCESS(f'Filtered to {len(filtered_data)} contracts with complete data'))

        if not filtered_data:
            self.stdout.write(self.style.WARNING('No contracts found with sufficient data'))
            return

        # Create DataFrame and save to Excel
        df = pd.DataFrame(filtered_data)
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save to Excel with multiple sheets
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Main contracts sheet
            df.to_excel(writer, sheet_name='Contracts', index=False)
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total Contracts Found',
                    'Contracts with Complete Data',
                    'Keywords Searched',
                    'Minimum Fields Required',
                    'Export Date'
                ],
                'Value': [
                    contracts.count(),
                    len(filtered_data),
                    ', '.join(keywords),
                    min_fields,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

        self.stdout.write(
            self.style.SUCCESS(f'Excel file generated successfully: {output_file}')
        )
        self.stdout.write(f'File contains {len(filtered_data)} contracts with complete data')
        self.stdout.write(f'File size: {os.path.getsize(output_file) / 1024:.1f} KB')
