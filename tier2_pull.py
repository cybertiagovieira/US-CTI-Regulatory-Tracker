import base64
import hashlib
import hmac
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
import uuid
import os

API_KEY = os.environ.get("SB_API_KEY")
SHARED_KEY = os.environ.get("SB_SHARED_KEY")
BASE_URL = "https://api.silobreaker.com/"

today = datetime.today()
first_day_current_month = today.replace(day=1)
last_day_prev_month = first_day_current_month - timedelta(days=1)
first_day_prev_month = last_day_prev_month.replace(day=1)

start_date = first_day_prev_month.strftime('%Y-%m-%d')
end_date = last_day_prev_month.strftime('%Y-%m-%d')

# Targeting Tier 2 Regulatory Bodies
QUERY = '(publisher:"New York State Department of Financial Services" OR publisher:"FINRA" OR publisher:"Financial Industry Regulatory Authority" OR publisher:"National Futures Association") AND (doctype:"Press Release" OR doctype:"Notice" OR doctype:"Guidance")'

def _generateMessage(method, url, body):
    return f"{method} {url}\n{body}"

def _createUrlWithDigest(url, message):
    digest = base64.b64encode(hmac.new(SHARED_KEY.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
    separator = "&" if "?" in url else "?"
    return f"{BASE_URL}{url}{separator}apiKey={API_KEY}&digest={urllib.parse.quote(digest)}"

def fetch_tier2_data():
    if not API_KEY or not SHARED_KEY:
        print("Fatal: API Credentials missing from environment variables.")
        return []

    url_path = f"search?q={urllib.parse.quote(QUERY)}&fromdate={start_date}&todate={end_date}&pagesize=100"
    message = _generateMessage("GET", url_path, "")
    url_with_digest = _createUrlWithDigest(url_path, message)
    
    req = urllib.request.Request(url_with_digest)
    try:
        response = urllib.request.urlopen(req)
        data = json.loads(response.read().decode("utf-8"))
    except Exception as e:
        print(f"Silobreaker API Error: {e}")
        return []

    items = data.get("Items", [])
    print(f"Silobreaker Items Extracted: {len(items)}")
    
    payload = []
    for item in items:
        publisher = item.get("Publisher", "")
        if "New York" in publisher or "NYDFS" in publisher:
            agency = "NYDFS"
        elif "FINRA" in publisher or "Regulatory Authority" in publisher:
            agency = "FINRA"
        elif "Futures" in publisher or "NFA" in publisher:
            agency = "NFA"
        else:
            agency = "Other/SRO"
        
        pub_date_str = item.get("PublicationDate", start_date)
        date_obj = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))

        description = item.get("Description", "")
        teaser = item.get("Teaser", "")

        payload.append({
            "id": f"SB-{uuid.uuid4().hex[:6]}",
            "date": date_obj.strftime('%Y-%m-%d'),
            "agency": agency,
            "type": "Enforcement Action" if "enforcement" in description.lower() or "fine" in description.lower() else "Guidance/Circular",
            "title": description,
            "summary": teaser
        })
    return payload

if __name__ == "__main__":
    tier2_data = fetch_tier2_data()
    print(f"Total Tier 2 Records Staged: {len(tier2_data)}")
    with open('latest_tier2_pull.json', 'w') as outfile:
        json.dump(tier2_data, outfile, indent=2)
