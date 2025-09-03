#!/usr/bin/env python3
"""
Fixed Army Contracts Filter Script
Works with the actual database structure.
"""

import sqlite3
import pandas as pd
import argparse
import os
from datetime import datetime


def connect_to_database(db_path='db.sqlite3'):
    """Connect to the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return None


def search_contracts(conn, keywords):
    """Search for contracts containing the specified keywords."""
    if not conn:
        return pd.DataFrame()
    
    # Build the search query - only use tables that actually exist
    search_conditions = []
    params = []
    
    for keyword in keywords:
        search_conditions.append("""
            (c.raw_text LIKE ? OR 
             c.contract_no LIKE ? OR
             od.organisation_name LIKE ? OR
             od.department LIKE ? OR
             od.ministry LIKE ? OR
             bd.address LIKE ? OR
             pa.address LIKE ?)
        """)
        # Add the keyword parameter 7 times (for each field)
        params.extend([f'%{keyword}%'] * 7)
    
    # Combine all conditions with OR
    where_clause = ' OR '.join(search_conditions)
    
    query = f"""
    SELECT DISTINCT
        c.id,
        c.contract_no,
        c.generated_date,
        c.raw_text,
        od.type as org_type,
        od.ministry,
        od.department,
        od.organisation_name,
        od.office_zone,
        bd.designation as buyer_designation,
        bd.contact_no as buyer_contact,
        bd.email as buyer_email,
        bd.gstin as buyer_gstin,
        bd.address as buyer_address,
        pa.role as pa_role,
        pa.payment_mode,
        pa.designation as pa_designation,
        pa.email as pa_email,
        pa.gstin as pa_gstin,
        pa.address as pa_address,
        fa.ifd_concurrence,
        fa.admin_approval_designation,
        fa.financial_approval_designation
    FROM cont_record_contract c
    LEFT JOIN cont_record_organisationdetail od ON c.id = od.contract_id
    LEFT JOIN cont_record_buyerdetail bd ON c.id = bd.contract_id
    LEFT JOIN cont_record_payingauthority pa ON c.id = pa.contract_id
    LEFT JOIN cont_record_financialapproval fa ON c.id = fa.contract_id
    WHERE {where_clause}
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        print(f"❌ Error executing search query: {e}")
        return pd.DataFrame()


def filter_complete_contracts(df, min_fields=3):
    """Filter contracts that have a minimum number of non-empty fields."""
    if df.empty:
        return df
    
    # Define the fields to count (only fields that exist in our query)
    fields_to_count = [
        'org_type', 'ministry', 'department', 'organisation_name', 'office_zone',
        'buyer_designation', 'buyer_contact', 'buyer_email', 'buyer_gstin', 'buyer_address',
        'pa_role', 'payment_mode', 'pa_designation', 'pa_email', 'pa_gstin', 'pa_address',
        'ifd_concurrence', 'admin_approval_designation', 'financial_approval_designation'
    ]
    
    # Count non-empty fields for each contract
    field_counts = df[fields_to_count].notna().sum(axis=1)
    
    # Filter contracts that meet the minimum field requirement
    filtered_df = df[field_counts >= min_fields].copy()
    
    print(f"📊 Found {len(df)} contracts, {len(filtered_df)} meet minimum field requirement ({min_fields})")
    
    # If no contracts meet the requirement, return all contracts with at least some data
    if len(filtered_df) == 0 and len(df) > 0:
        print(f"⚠️  No contracts meet minimum requirement. Returning all contracts with any data...")
        # Return contracts that have at least contract_no and some other data
        basic_fields = ['contract_no', 'raw_text']
        basic_counts = df[basic_fields].notna().sum(axis=1)
        filtered_df = df[basic_counts >= 1].copy()
        print(f"📊 Returning {len(filtered_df)} contracts with basic data")
    
    return filtered_df


def clean_text_for_excel(text):
    """Clean text to remove illegal characters for Excel."""
    if not text:
        return ''
    
    # Remove or replace illegal characters
    import re
    # Remove control characters except newlines and tabs
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', str(text))
    # Replace other problematic characters
    text = text.replace('\x00', '').replace('\x01', '').replace('\x02', '').replace('\x03', '')
    text = text.replace('\x04', '').replace('\x05', '').replace('\x06', '').replace('\x07', '')
    text = text.replace('\x08', '').replace('\x0B', '').replace('\x0C', '').replace('\x0E', '')
    text = text.replace('\x0F', '').replace('\x10', '').replace('\x11', '').replace('\x12', '')
    text = text.replace('\x13', '').replace('\x14', '').replace('\x15', '').replace('\x16', '')
    text = text.replace('\x17', '').replace('\x18', '').replace('\x19', '').replace('\x1A', '')
    text = text.replace('\x1B', '').replace('\x1C', '').replace('\x1D', '').replace('\x1E', '')
    text = text.replace('\x1F', '').replace('\x7F', '')
    
    return text

def create_excel_output(df, output_file, keywords, min_fields):
    """Create Excel file with contracts and summary data."""
    if df.empty:
        print("⚠️  No contracts found with sufficient data")
        return
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Prepare contracts data
    contracts_data = []
    
    for _, contract in df.iterrows():
        contract_dict = {
            'Contract No': clean_text_for_excel(contract['contract_no'] or ''),
            'Generated Date': clean_text_for_excel(contract['generated_date'] or ''),
            'Raw Text': clean_text_for_excel((contract['raw_text'][:1000] + '...') if contract['raw_text'] and len(contract['raw_text']) > 1000 else contract['raw_text'] or ''),
            'Organization Type': clean_text_for_excel(contract['org_type'] or ''),
            'Ministry': clean_text_for_excel(contract['ministry'] or ''),
            'Department': clean_text_for_excel(contract['department'] or ''),
            'Organization Name': clean_text_for_excel(contract['organisation_name'] or ''),
            'Office Zone': clean_text_for_excel(contract['office_zone'] or ''),
            'Buyer Designation': clean_text_for_excel(contract['buyer_designation'] or ''),
            'Buyer Contact': clean_text_for_excel(contract['buyer_contact'] or ''),
            'Buyer Email': clean_text_for_excel(contract['buyer_email'] or ''),
            'Buyer GSTIN': clean_text_for_excel(contract['buyer_gstin'] or ''),
            'Buyer Address': clean_text_for_excel(contract['buyer_address'] or ''),
            'PA Role': clean_text_for_excel(contract['pa_role'] or ''),
            'PA Payment Mode': clean_text_for_excel(contract['payment_mode'] or ''),
            'PA Designation': clean_text_for_excel(contract['pa_designation'] or ''),
            'PA Email': clean_text_for_excel(contract['pa_email'] or ''),
            'PA GSTIN': clean_text_for_excel(contract['pa_gstin'] or ''),
            'PA Address': clean_text_for_excel(contract['pa_address'] or ''),
            'IFD Concurrence': 'Yes' if contract['ifd_concurrence'] else 'No',
            'Admin Approval Designation': clean_text_for_excel(contract['admin_approval_designation'] or ''),
            'Financial Approval Designation': clean_text_for_excel(contract['financial_approval_designation'] or ''),
        }
        
        contracts_data.append(contract_dict)
    
    # Create Excel file
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Contracts sheet
        contracts_df = pd.DataFrame(contracts_data)
        contracts_df.to_excel(writer, sheet_name='Contracts', index=False)
        
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
                len(df),
                len(contracts_data),
                ', '.join(keywords),
                min_fields,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    print(f"📁 Excel file generated successfully: {output_file}")
    print(f"📊 File contains {len(contracts_data)} contracts with complete data")
    print(f"💾 File size: {os.path.getsize(output_file) / 1024:.1f} KB")


def main():
    parser = argparse.ArgumentParser(description='Filter contracts for army-related keywords and generate Excel file')
    parser.add_argument('--output', '-o', default='army_contracts_full_data.xlsx',
                       help='Output Excel file name (default: army_contracts_full_data.xlsx)')
    parser.add_argument('--keywords', '-k', 
                       default='India Army,HQ,Headquarters,Armd,ARMD,army,ARMY,headquarters',
                       help='Comma-separated keywords to search for')
    parser.add_argument('--min-fields', '-m', type=int, default=3,
                       help='Minimum number of required fields that must be present (default: 3)')
    parser.add_argument('--database', '-d', default='db.sqlite3',
                       help='SQLite database file path (default: db.sqlite3)')
    
    args = parser.parse_args()
    
    # Parse keywords
    keywords = [kw.strip() for kw in args.keywords.split(',')]
    
    print(f"🔍 Searching for contracts with keywords: {keywords}")
    print(f"📊 Minimum required fields: {args.min_fields}")
    print(f"🗄️  Database: {args.database}")
    
    # Connect to database
    conn = connect_to_database(args.database)
    if not conn:
        return
    
    try:
        # Search for contracts
        print("🔍 Searching contracts...")
        contracts_df = search_contracts(conn, keywords)
        
        if contracts_df.empty:
            print("❌ No contracts found matching the keywords")
            return
        
        print(f"📋 Found {len(contracts_df)} contracts matching keywords")
        
        # Filter contracts with complete data
        print("🔍 Filtering contracts with complete data...")
        filtered_df = filter_complete_contracts(contracts_df, args.min_fields)
        
        if filtered_df.empty:
            print("⚠️  No contracts found with sufficient data")
            return
        
        # Create Excel output
        print("📁 Creating Excel file...")
        create_excel_output(filtered_df, args.output, keywords, args.min_fields)
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()
