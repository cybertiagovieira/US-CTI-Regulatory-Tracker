import base64
import hashlib
import hmac
import json
import urllib.request
from urllib import parse
from datetime import datetime, timedelta
import uuid
import os

apikey = os.environ.get("SB_API_KEY", "").strip()
sharedkey = os.environ.get("SB_SHARED_KEY", "").strip()
baseurl = "https://api.silobreaker.com/"

# --- Date range: previous calendar month ---
today = datetime.today()
first_day_current_month = today.replace(day=1)
last_day_prev_month = first_day_current_month - timedelta(days=1)
first_day_prev_month = last_day_prev_month.replace(day=1)
start_date = first_day_prev_month.strftime('%Y-%m-%d')
end_date = last_day_prev_month.strftime('%Y-%m-%d')

# --- Silobreaker entity names (must match platform entity database exactly) ---
# Targets: NYDFS, FINRA, NFA — agencies not covered by Tier 1 Federal Register pull
# Date range is embedded inline in the query string, not as URL params
QUERY = (
    '(Organization:"FINRA Financial Industry Regulatory Authority" OR '
    'Organization:"National Futures Association" OR '
    'Organization:"New York State Department of Financial Services") AND '
    'language:en AND '
    f'fromdate:"{start_date}" AND todate:"{end_date}"'
)

# --- Agency resolution map ---
AGENCY_MAP = {
    "new york": "NYDFS",
    "nydfs": "NYDFS",
    "finra": "FINRA",
    "financial industry regulatory": "FINRA",
    "futures association": "NFA",
    "nfa": "NFA"
}

# --- Type inference from document title/description ---
TYPE_KEYWORDS = {
    "Final Rule":           ["final rule", "adopted rule", "adopts rule"],
    "Enforcement Action":   ["enforcement", "fine", "penalty", "sanction", "censure",
                             "suspended", "barred", "expelled", "cease and desist"],
    "NPRM":                 ["proposed rule", "notice of proposed", "request for comment",
                             "rfc", "concept release"],
    "Guidance/Circular":    ["guidance", "circular", "notice", "information memo",
                             "regulatory notice", "faq", "interpretive"],
    "Examination Priority": ["examination priority", "exam priority", "supervisory priority"],
    "Speech/Statement":     ["speech", "statement", "remarks", "testimony", "address"]
}


def _create_url(path: str) -> str:
    """
    Build the full URL with source=ApiKey appended.
    source=ApiKey MUST be included before HMAC signing — this matches
    the official Silobreaker Python sample pattern.
    """
    url = baseurl + path
    sep = "&" if "?" in url else "?"
    return url + sep + "source=ApiKey"


def _sign_url(url: str) -> str:
    """
    Compute HMAC-SHA1 digest and append apiKey + digest to URL.
    Algorithm: SHA-1 per official Silobreaker API documentation.
    """
    message = ("GET " + url).encode()
    # FIX: SHA-1, not SHA-512
    digest = base64.b64encode(
        hmac.new(sharedkey.encode(), message, digestmod=hashlib.sha1).digest()
    ).decode()
    return url + "&apiKey=" + apikey + "&digest=" + parse.quote(digest)


def _infer_type(text: str) -> str:
    """Map document description text to our canonical action type hierarchy."""
    text_lower = text.lower()
    for action_type, keywords in TYPE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return action_type
    return "Guidance/Circular"  # Default for unclassified regulatory communications


def _resolve_agency(publisher: str) -> str:
    pub_lower = publisher.lower()
    for key, acronym in AGENCY_MAP.items():
        if key in pub_lower:
            return acronym
    return "Other/SRO"


def fetch_tier2_data() -> list:
    if not apikey or not sharedkey:
        print("Fatal: SB_API_KEY and SB_SHARED_KEY environment variables are required.")
        return []

    # FIX: parameter name is 'query' (not 'q'), and 'extras=documentTeasers'
    # must be requested to receive teaser text in the response.
    # Date range is embedded in the query string, not as separate URL params.
    params = parse.urlencode({
        "query": QUERY,
        "pageSize": 100,
        "extras": "documentTeasers",   # FIX: required to populate Teaser field
        "sortBy": "publicationdate",
        "sortDirection": "desc"
    })

    path = f"v2/documents/search?{params}"
    base = _create_url(path)           # FIX: source=ApiKey appended before signing
    signed = _sign_url(base)           # FIX: SHA-1 HMAC over full URL

    try:
        response = urllib.request.urlopen(urllib.request.Request(signed))
        data = json.loads(response.read().decode("utf-8"))
    except Exception as e:
        print(f"Silobreaker API error: {e}")
        return []

    items = data.get("Items", [])
    print(f"Silobreaker raw items received: {len(items)}")

    payload = []
    for item in items:
        publisher = item.get("Publisher", "")
        agency = _resolve_agency(publisher)

        description = item.get("Description", "")
        action_type = _infer_type(description)

        # Teaser is returned under Extras.documentTeasers when extras param is set
        teaser = ""
        extras = item.get("Extras", {})
        if extras:
            teaser_obj = extras.get("documentTeasers", {})
            if isinstance(teaser_obj, dict):
                teaser = teaser_obj.get("Teaser", "")
            elif isinstance(teaser_obj, str):
                teaser = teaser_obj
        # Fallback: some API versions return Teaser at root level
        if not teaser:
            teaser = item.get("Teaser", "")

        pub_date_str = item.get("PublicationDate", start_date)
        try:
            date_obj = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
            date_str = date_obj.strftime('%Y-%m-%d')
        except ValueError:
            date_str = start_date

        source_url = item.get("SourceUrl", "")
        silobreaker_url = item.get("SilobreakerUrl", "")

        payload.append({
            "id": f"SB-{uuid.uuid4().hex[:6]}",
            "date": date_str,
            "agency": agency,
            "type": action_type,
            "title": description,
            "summary": teaser,
            "source_url": source_url or silobreaker_url  # FIX: capture attribution URL
        })

    print(f"Tier 2 records staged: {len(payload)}")
    return payload


if __name__ == "__main__":
    tier2_data = fetch_tier2_data()
    with open("latest_tier2_pull.json", "w") as f:
        json.dump(tier2_data, f, indent=2)
