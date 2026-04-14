import requests
import json
from datetime import datetime, timedelta

# --- Agencies scoped to banking/financial regulation only ---
# "treasury-department" REMOVED: too broad, pulls TTB (wine/tobacco) entries
# "internal-revenue-service" SCOPED: IRS kept but filtered post-fetch to
#   digital asset / crypto / BSA-relevant rules only
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
    # FCA (Farm Credit Administration) omitted: limited relevance to global banking operations
    # Treasury (treasury-department) omitted: slug captures TTB and other non-banking sub-agencies
}

# Sub-agencies that appear under broad slugs and are NOT relevant to banking regulation.
# Entries whose agencies array contains any of these strings are dropped post-fetch.
IRRELEVANT_SUB_AGENCIES = [
    "alcohol and tobacco",
    "tax and trade bureau",
    "ttb",
    "forest service",
    "natural resources",
    "farm service"
]

# IRS entries are kept only if the title contains at least one of these terms.
# This scopes IRS to digital asset reporting obligations and AML/BSA-relevant rules.
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
    """Return True if the entry belongs to a non-banking sub-agency."""
    combined = " ".join(item_agencies).lower()
    return any(term in combined for term in IRRELEVANT_SUB_AGENCIES)


def _is_irs_relevant(title: str) -> bool:
    """For IRS entries, return True only if title matches banking/crypto scope."""
    title_lower = title.lower()
    return any(kw in title_lower for kw in IRS_RELEVANCE_KEYWORDS)


def _resolve_agency(item_agencies: list) -> str:
    """Map Federal Register agency names to our canonical acronyms."""
    for acronym, slug in AGENCY_SLUGS.items():
        slug_words = slug.replace("-", " ")
        if any(slug_words in raw.lower() for raw in item_agencies):
            return acronym
    return "Multiple/Treasury"


def fetch_fr_data(start_date: str, end_date: str) -> list:
    base_url = "https://www.federalregister.gov/api/v1/articles.json"

    agency_params = "&".join(
        f"conditions[agencies][]={slug}" for slug in AGENCY_SLUGS.values()
    )
    # RULE = Final Rule, PRORULE = Proposed Rule (NPRM)
    type_query = "conditions[type][]=RULE&conditions[type][]=PRORULE"
    date_query = (
        f"conditions[publication_date][gte]={start_date}"
        f"&conditions[publication_date][lte]={end_date}"
    )
    # FIX: request html_url field for source attribution
    fields = "fields[]=document_number&fields[]=publication_date&fields[]=title&fields[]=abstract&fields[]=agencies&fields[]=type&fields[]=html_url"

    full_url = f"{base_url}?{agency_params}&{type_query}&{date_query}&{fields}&per_page=1000"

    response = requests.get(full_url)
    if response.status_code != 200:
        print(f"Federal Register API error: HTTP {response.status_code}")
        return []

    results = response.json().get("results", [])
    payload = []
    skipped = 0

    for item in results:
        item_agencies = [agency.get("raw_name", "") for agency in item.get("agencies", [])]
        title = item.get("title", "")
        matched_agency = _resolve_agency(item_agencies)

        # FIX: drop TTB and other non-banking Treasury sub-agency entries
        if _is_irrelevant_sub_agency(item_agencies):
            skipped += 1
            continue

        # FIX: restrict IRS entries to banking/crypto-relevant rules only
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
            "source_url": item.get("html_url", "")   # FIX: capture source URL
        }
        payload.append(entry)

    print(f"Federal Register: {len(payload)} entries kept, {skipped} dropped (irrelevant sub-agencies/IRS scope)")
    return payload


if __name__ == "__main__":
    start_date, end_date = get_previous_month_dates()
    raw_intelligence = fetch_fr_data(start_date, end_date)
    with open("latest_pull.json", "w") as f:
        json.dump(raw_intelligence, f, indent=2)
