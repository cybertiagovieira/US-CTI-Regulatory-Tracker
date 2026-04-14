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
baseurl = "https://api.silobreaker.com/v2/documents/search"

today = datetime.today()
first_day_current_month = today.replace(day=1)
last_day_prev_month = first_day_current_month - timedelta(days=1)
first_day_prev_month = last_day_prev_month.replace(day=1)

start_date = first_day_prev_month.strftime('%Y-%m-%d')
end_date = last_day_prev_month.strftime('%Y-%m-%d')

QUERY = '(publisher:"New York State Department of Financial Services" OR publisher:"FINRA" OR publisher:"Financial Industry Regulatory Authority" OR publisher:"National Futures Association") AND (doctype:"Press Release" OR doctype:"Notice" OR doctype:"Guidance")'

def fetch_tier2_data():
    if not apikey or not sharedkey:
        print("Fatal: API Credentials missing from environment variables.")
        return []

    # Construct parameter string
    params = {
        'q': QUERY,
        'fromdate': start_date,
        'todate': end_date,
        'pageSize': 100
    }
    
    query_string = parse.urlencode(params)
    raw_url = f"{baseurl}?{query_string}"
    
    # Encode URL per official Silobreaker Python standard
    url = parse.quote(raw_url, safe=":/?&=")
    
    verb = "GET"
    message = verb + " " + url

    # Generate HMAC-SHA512 Signature
    hmac_sha512 = hmac.new(sharedkey.encode(), message.encode(), digestmod=hashlib.sha512)
    digest = base64.b64encode(hmac_sha512.digest())

    # Construct final URL with authentication parameters
    sep = '&' if '?' in url else '?'
    final_url = url + sep + "apiKey=" + apikey + "&digest=" + parse.quote(digest.decode())

    req = urllib.request.Request(final_url)
    
    try:
        response = urllib.request.urlopen(req)
        response_json = response.read()
        data = json.loads(response_json.decode("utf-8"))
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
