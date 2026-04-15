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

today = datetime.today()
first_day_current_month = today.replace(day=1)
last_day_prev_month = first_day_current_month - timedelta(days=1)
first_day_prev_month = last_day_prev_month.replace(day=1)
start_date = first_day_prev_month.strftime('%Y-%m-%d')
end_date = last_day_prev_month.strftime('%Y-%m-%d')

# Deploy relative temporal offset. Restrict to specific document types.
QUERY = (
    '(organization:"FINRA Financial Industry Regulatory Authority" OR '
    'organization:"National Futures Association" OR '
    'governmentbody:"New York State Department of Financial Services") AND '
    '(doctype:"Press Release" OR doctype:"Report" OR doctype:"Fact Sheet") AND '
    'fromdate:"2026-01-01"'
)

AGENCY_MAP = {
    "new york": "NYDFS",
    "nydfs": "NYDFS",
    "finra": "FINRA",
    "financial industry regulatory": "FINRA",
    "futures association": "NFA",
    "nfa": "NFA"
}

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
    url = baseurl + path
    sep = "&" if "?" in url else "?"
    return url + sep + "source=ApiKey"

def _sign_url(url: str) -> str:
    message = ("GET " + url).encode()
    digest = base64.b64encode(
        hmac.new(sharedkey.encode(), message, digestmod=hashlib.sha1).digest()
    ).decode()
    return url + "&apiKey=" + apikey + "&digest=" + parse.quote(digest)

def _infer_type(text: str) -> str:
    text_lower = text.lower()
    for action_type, keywords in TYPE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return action_type
    return "Guidance/Circular"

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

    params = parse.urlencode({
        "query": QUERY,
        "pageSize": 100,
        "extras": "documentTeasers",
        "sortBy": "publicationdate",
        "sortDirection": "desc"
    })

    path = f"v2/documents/search?{params}"
    base = _create_url(path)
    signed = _sign_url(base)

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
        pub_date_str = item.get("PublicationDate", "")
        try:
            date_obj = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
            date_str = date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue

        publisher = item.get("Publisher", "")
        agency = _resolve_agency(publisher)

        description = item.get("Description", "")
        action_type = _infer_type(description)

        teaser = ""
        extras = item.get("Extras", {})
        if extras:
            teaser_obj = extras.get("documentTeasers", {})
            if isinstance(teaser_obj, dict):
                teaser = teaser_obj.get("Teaser", "")
            elif isinstance(teaser_obj, str):
                teaser = teaser_obj
        if not teaser:
            teaser = item.get("Teaser", "")

        source_url = item.get("SourceUrl", "")
        silobreaker_url = item.get("SilobreakerUrl", "")

        payload.append({
            "id": f"SB-{uuid.uuid4().hex[:6]}",
            "date": date_str,
            "agency": agency,
            "type": action_type,
            "title": description,
            "summary": teaser,
            "source_url": source_url or silobreaker_url
        })

    print(f"Tier 2 records staged: {len(payload)}")
    return payload

if __name__ == "__main__":
    tier2_data = fetch_tier2_data()
    with open("latest_tier2_pull.json", "w") as f:
        json.dump(tier2_data, f, indent=2)
