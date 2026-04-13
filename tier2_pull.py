import requests
from bs4 import BeautifulSoup
import feedparser
from datetime import datetime, timedelta
import json
import uuid

target_month = (datetime.today().replace(day=1) - timedelta(days=1)).strftime('%Y-%m')

# Modern browser spoofing to bypass basic bot mitigation
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5'
}

def fetch_nydfs():
    payload = []
    url = "https://www.dfs.ny.gov/industry_guidance/industry_letters"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        print(f"NYDFS HTTP Status: {response.status_code}")
        if response.status_code != 200: return payload
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.find_all('div', class_='views-row')
        print(f"NYDFS DOM Nodes Found: {len(articles)}")
        for article in articles:
            time_tag = article.find('time')
            if not time_tag: continue
            date_obj = datetime.strptime(time_tag.text.strip(), '%B %d, %Y')
            if date_obj.strftime('%Y-%m') == target_month:
                payload.append({
                    "id": f"NYDFS-{uuid.uuid4().hex[:6]}",
                    "date": date_obj.strftime('%Y-%m-%d'),
                    "agency": "NYDFS",
                    "type": "Guidance/Circular",
                    "title": article.find('a').text.strip(),
                    "summary": "Industry Letter published by NYDFS. Review source for full text."
                })
    except Exception as e: print(f"NYDFS Error: {e}")
    return payload

def fetch_finra():
    payload = []
    url = "https://www.finra.org/rss/finra-notices"
    try:
        feed = feedparser.parse(url)
        print(f"FINRA RSS Entries Found: {len(feed.entries)}")
        for entry in feed.entries:
            pub_date = datetime(*entry.published_parsed[:6])
            if pub_date.strftime('%Y-%m') == target_month:
                payload.append({
                    "id": f"FINRA-{uuid.uuid4().hex[:6]}",
                    "date": pub_date.strftime('%Y-%m-%d'),
                    "agency": "FINRA",
                    "type": "Enforcement Action" if "enforcement" in entry.title.lower() else "Guidance/Circular",
                    "title": entry.title,
                    "summary": entry.description[:250] + "..."
                })
    except Exception as e: print(f"FINRA Error: {e}")
    return payload

def fetch_nfa():
    payload = []
    url = "https://www.nfa.futures.org/news/newsNotice.asp"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        print(f"NFA HTTP Status: {response.status_code}")
        if response.status_code != 200: return payload
        soup = BeautifulSoup(response.content, 'html.parser')
        rows = soup.find_all('tr')
        print(f"NFA DOM Nodes Found: {len(rows)}")
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                try:
                    date_obj = datetime.strptime(cols[0].text.strip(), '%m/%d/%Y')
                    if date_obj.strftime('%Y-%m') == target_month:
                        payload.append({
                            "id": f"NFA-{uuid.uuid4().hex[:6]}",
                            "date": date_obj.strftime('%Y-%m-%d'),
                            "agency": "NFA",
                            "type": "Guidance/Circular",
                            "title": cols[1].text.strip(),
                            "summary": "NFA Notice to Members published. Review source for full text."
                        })
                except ValueError: continue
    except Exception as e: print(f"NFA Error: {e}")
    return payload

if __name__ == "__main__":
    tier2_data = []
    tier2_data.extend(fetch_nydfs())
    tier2_data.extend(fetch_finra())
    tier2_data.extend(fetch_nfa())
    print(f"Total Tier 2 Records Extracted: {len(tier2_data)}")
    with open('latest_tier2_pull.json', 'w') as outfile:
        json.dump(tier2_data, outfile, indent=2)
