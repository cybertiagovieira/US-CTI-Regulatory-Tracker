import base64
import hashlib
import hmac
import json
import urllib.request
import urllib.parse
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

def _fetch(req):
    response = urllib.request.urlopen(req)
    response_json = response.read()
    return json.loads(response_json.decode("utf-8"))

def _createUrl(url):
    out_url = baseurl + url
    if "?" not in out_url:
        out_url += "?"
    else:
        out_url += "&"
    return out_url

def _generateMessage(method, url, body):
    return method + " " + url + "\n" + body

def _createUrlWithDigest(url, message):
    digest = base64.b64encode(hmac.new(sharedkey.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
    return _createUrl(url) + "apiKey=" + apikey + "&digest=" + urllib.parse.quote(digest)

def get(url):
    message = _generateMessage("GET", url, '')
    url_with_digest = _createUrlWithDigest(url, message)
    req = urllib.request.Request(url_with_digest)
    return _fetch(req)

def searchDocuments(query, params=None):
    url = "search"
    if params:
        p = dict(params)
        p['q'] = query
        url += "?" + urllib.parse.urlencode(p)
    return get(url)

def fetch_tier2_data():
    if not apikey or not sharedkey:
        print("Fatal: API Credentials missing from environment variables.")
        return []

    QUERY = '(publisher:"New York State Department of Financial Services" OR publisher:"FINRA" OR publisher:"Financial Industry Regulatory Authority" OR publisher:"National Futures Association") AND (doctype:"Press Release" OR doctype:"Notice" OR doctype:"Guidance")'
    
    params = {
        'fromdate': start_date,
        'todate': end_date,
        'pagesize': 100
    }
    
    try:
        data = searchDocuments(QUERY, params)
    except Exception as e:
        print(f"Silobreaker API Error: {e}")
        return []
    
    if not data:
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
