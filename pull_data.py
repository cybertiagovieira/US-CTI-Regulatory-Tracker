import os
import json
import time
import requests
from datetime import datetime, timedelta

MASTER_DATA_FILE = 'master_data.json'
LATEST_PULL_FILE = 'latest_pull.json'

def get_previous_month_dates():
    """Calculates the start and end dates of the previous month for dynamic execution."""
    today = datetime.today()
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    
    return (
        first_day_prev_month.strftime('%Y-%m-%d'),
        last_day_prev_month.strftime('%Y-%m-%d')
    )

def extract_paginated_data(base_url, query_params):
    """
    Executes recursive pagination to bypass 1000-record API limits.
    """
    all_records = []
    current_page = 1
    
    print("Initiating Paginated API Extraction...")

    while True:
        query_params['per_page'] = 1000
        query_params['page'] = current_page
        
        try:
            response = requests.get(base_url, params=query_params, timeout=15)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Fatal: API connection severed on page {current_page}. {e}")
            break

        results = payload.get('results', [])
        if not results:
            break  # Terminate if payload array is empty

        all_records.extend(results)
        
        # Federal Register specific pagination key
        total_pages = payload.get('total_pages', 1)
        if current_page >= total_pages:
            break  # Terminate if ceiling reached

        current_page += 1
        time.sleep(0.5)  # Enforce rate limit protection

    return all_records

def process_tier1():
    start_date, end_date = get_previous_month_dates()
    print(f"Targeting execution window: {start_date} to {end_date}")

    base_url = 'https://www.federalregister.gov/api/v1/documents.json'
    
    # Financial and Cyber regulatory agencies
    target_agencies = [
        'comptroller-of-the-currency',
        'federal-deposit-insurance-corporation',
        'federal-reserve-system',
        'securities-and-exchange-commission',
        'commodity-futures-trading-commission',
        'financial-crimes-enforcement-network',
        'consumer-financial-protection-bureau',
        'cybersecurity-and-infrastructure-security-agency'
    ]

    params = {
        'conditions[publication_date][gte]': start_date,
        'conditions[publication_date][lte]': end_date,
        'conditions[type][]': ['RULE', 'PRORULE', 'NOTICE'],
        'conditions[agencies][]': target_agencies
    }

    raw_data = extract_paginated_data(base_url, params)
    
    processed_records = []
    dropped = 0

    agency_map = {
        'Comptroller of the Currency': 'OCC',
        'Federal Deposit Insurance Corporation': 'FDIC',
        'Federal Reserve System': 'FRB',
        'Securities and Exchange Commission': 'SEC',
        'Commodity Futures Trading Commission': 'CFTC',
        'Financial Crimes Enforcement Network': 'FinCEN',
        'Consumer Financial Protection Bureau': 'CFPB',
        'Cybersecurity and Infrastructure Security Agency': 'CISA'
    }

    for item in raw_data:
        # Standardize Taxonomy Types
        fr_type = item.get('type')
        if fr_type == 'RULE':
            action_type = 'Final Rule'
        elif fr_type == 'PRORULE':
            action_type = 'NPRM'
        elif fr_type == 'NOTICE':
            action_type = 'Guidance/Circular'
        else:
            dropped += 1
            continue

        # Extract Agency Mapping
        agencies = item.get('agencies', [])
        agency_name = agencies[0].get('raw_name') if agencies else "Unknown"
        mapped_agency = agency_map.get(agency_name, 'Other/SRO')

        # --- SAFEGUARD FIX PLACEMENT ---
        doc_num = item.get('document_number')
        if not doc_num:
            dropped += 1
            continue # Skip malformed API records

        # Build Dashboard Payload Object
        record = {
            'id': f"FR-{doc_num}", # Inject the validated variable
            'date': item.get('publication_date'),
            'effective_date': item.get('effective_on', ''), 
            'agency': mapped_agency,
            'type': action_type,
            'severity': None,      
            'theme': None,         
            'target_sector': None, 
            'title': item.get('title'),
            'summary': item.get('abstract', ''), 
            'source_url': item.get('html_url'),
            'raw_text': '' 
        }

        # Store full text URL in raw_text field to be scraped by Tier 3 if needed
        text_url = item.get('body_html_url')
        if text_url:
            record['source_url'] = text_url 
            
        processed_records.append(record)

    print(f"Federal Register: {len(processed_records)} entries kept, {dropped} dropped")

    # Save artifact for Tier 2 reference
    with open(LATEST_PULL_FILE, 'w', encoding='utf-8') as f:
        json.dump(processed_records, f, indent=2)

    # Ingest directly into Master DB, bypassing duplicates
    try:
        with open(MASTER_DATA_FILE, 'r', encoding='utf-8') as f:
            master_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        master_data = []

    existing_ids = {d.get('id') for d in master_data if d.get('id')}
    new_additions = [r for r in processed_records if r['id'] not in existing_ids]

    master_data.extend(new_additions)

    with open(MASTER_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(master_data, f, indent=2)

if __name__ == "__main__":
    process_tier1()
