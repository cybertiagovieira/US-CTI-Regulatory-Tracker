import requests
import json
from datetime import datetime, timedelta
import calendar

AGENCY_SLUGS = {
    "OCC": "comptroller-of-the-currency",
    "FDIC": "federal-deposit-insurance-corporation",
    "FRB": "federal-reserve-system",
    "SEC": "securities-and-exchange-commission",
    "CFTC": "commodity-futures-trading-commission",
    "FinCEN": "financial-crimes-enforcement-network",
    "CFPB": "consumer-financial-protection-bureau",
    "OFAC": "foreign-assets-control-office",
    "Treasury": "treasury-department",
    "NCUA": "national-credit-union-administration",
    "FCA": "farm-credit-administration",
    "FHFA": "federal-housing-finance-agency",
    "IRS": "internal-revenue-service"
}

def get_previous_month_dates():
    today = datetime.today()
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    
    start_date = first_day_prev_month.strftime('%Y-%m-%d')
    end_date = last_day_prev_month.strftime('%Y-%m-%d')
    return start_date, end_date

def fetch_fr_data(start_date, end_date):
    base_url = "https://www.federalregister.gov/api/v1/articles.json"
    
    agency_params = [f"conditions[agencies][]={slug}" for slug in AGENCY_SLUGS.values()]
    agency_query = "&".join(agency_params)
    
    type_query = "conditions[type][]=RULE&conditions[type][]=PRORULE"
    date_query = f"conditions[publication_date][gte]={start_date}&conditions[publication_date][lte]={end_date}"
    
    full_url = f"{base_url}?{agency_query}&{type_query}&{date_query}&per_page=1000"
    
    response = requests.get(full_url)
    if response.status_code != 200:
        return []
        
    data = response.json()
    results = data.get('results', [])
    
    dashboard_payload = []
    
    for item in results:
        item_agencies = [agency['raw_name'] for agency in item.get('agencies', [])]
        matched_agency = "Multiple/Treasury"
        for acronym, slug in AGENCY_SLUGS.items():
            if any(slug.replace("-", " ") in raw.lower() for raw in item_agencies):
                matched_agency = acronym
                break
                
        doc_type = "Final Rule" if item.get('type') == "Rule" else "NPRM"
        
        entry = {
            "id": item.get('document_number'),
            "date": item.get('publication_date'),
            "agency": matched_agency,
            "type": doc_type,
            "title": item.get('title'),
            "summary": item.get('abstract', 'No summary provided by agency.')
        }
        dashboard_payload.append(entry)
        
    return dashboard_payload

if __name__ == "__main__":
    start_date, end_date = get_previous_month_dates()
    raw_intelligence = fetch_fr_data(start_date, end_date)
    
    with open('latest_pull.json', 'w') as outfile:
        json.dump(raw_intelligence, outfile, indent=2)
