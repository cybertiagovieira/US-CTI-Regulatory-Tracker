import requests
import json
from datetime import datetime, timedelta

AGENCY_SLUGS = {
    "OCC":    "comptroller-of-the-currency",
    "FDIC":   "federal-deposit-insurance-corporation",
    "FRB":    "federal-reserve-system",
    "SEC":    "securities-and-exchange-commission",
    "CFTC":   "commodity-futures-trading-commission",
    "FinCEN": "financial-crimes-enforcement-network",
    "CFPB":   "consumer-financial-protection-bureau",
    "OFAC":   "foreign-assets-control-office",
    "NCUA":   "national-credit-union-administration",
    "FHFA":   "federal-housing-finance-agency",
    "IRS":    "internal-revenue-service"
}

IRRELEVANT_SUB_AGENCIES = [
    "alcohol and tobacco",
    "tax and trade bureau",
    "ttb",
    "forest service",
    "natural resources",
    "farm service"
]

IRS_RELEVANCE_KEYWORDS = [
    "digital asset", "cryptocurrency", "virtual currency", "crypto",
    "foreign account", "fbar", "fatca", "bsa", "bank secrecy",
    "reporting", "broker", "1099-da"
]

def get_previous_month_dates():
    today = datetime.today()
    first_day_current_month = today.replace(day=1)
    last_day_prev_month = first_day_current_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    return (
        first_day_prev_month.strftime('%Y-%m-%d'),
        last_day_prev_month.strftime('%Y-%m-%d')
    )

def _is_irrelevant_sub_agency(item_agencies: list) -> bool:
    combined = " ".join(item_agencies).lower()
    return any(term in combined for term in IRRELEVANT_SUB_AGENCIES)

def _is_irs_relevant(title: str) -> bool:
    title_lower = title.lower()
    return any(kw in title_lower for kw in IRS_RELEVANCE_KEYWORDS)

def _resolve_agency(item_agencies: list) -> str:
    for acronym, slug in AGENCY_SLUGS.items():
        slug_words = slug.replace("-", " ")
        if any(slug_words in raw.lower() for raw in item_agencies):
            return acronym
    return "Multiple/Treasury"

def fetch_fr_data(start_date: str, end_date: str) -> list:
    base_url = "https://www.federalregister.gov/api/v1/articles.json"
    agency_params = "&".join(f"conditions[agencies][]={slug}" for slug in AGENCY_SLUGS.values())
    type_query = "conditions[type][]=RULE&conditions[type][]=PRORULE"
    date_query = f"conditions[publication_date][gte]={start_date}&conditions[publication_date][lte]={end_date}"
    fields = "fields[]=document_number&fields[]=publication_date&fields[]=title&fields[]=abstract&fields[]=agencies&fields[]=type&fields[]=html_url"
    full_url = f"{base_url}?{agency_params}&{type_query}&{date_query}&{fields}&per_page=1000"

    try:
        # TIMEOUT IMPLEMENTED CORRECTLY HERE
        response = requests.get(full_url, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"Federal Register API error: {e}")
        return []

    results = response.json().get("results", [])
    payload = []
    skipped = 0

    for item in results:
        item_agencies = [agency.get("raw_name", "") for agency in item.get("agencies", [])]
        title = item.get("title", "")
        matched_agency = _resolve_agency(item_agencies)

        if _is_irrelevant_sub_agency(item_agencies):
            skipped += 1
            continue

        if matched_agency == "IRS" and not _is_irs_relevant(title):
            skipped += 1
            continue

        doc_type = "Final Rule" if item.get("type") == "Rule" else "NPRM"

        entry = {
            "id": item.get("document_number"),
            "date": item.get("publication_date"),
            "agency": matched_agency,
            "type": doc_type,
            "title": title,
            "summary": item.get("abstract", "No summary provided by agency."),
            "source_url": item.get("html_url", "")
        }
        payload.append(entry)

    print(f"Federal Register: {len(payload)} entries kept, {skipped} dropped")
    return payload

if __name__ == "__main__":
    start_date, end_date = get_previous_month_dates()
    raw_intelligence = fetch_fr_data(start_date, end_date)
    with open("latest_pull.json", "w") as f:
        json.dump(raw_intelligence, f, indent=2)