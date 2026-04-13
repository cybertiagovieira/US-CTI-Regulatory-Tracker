import requests
from bs4 import BeautifulSoup
import feedparser
from datetime import datetime, timedelta
import json
import uuid

# Calculate previous month boundaries
today = datetime.today()
first_day_current_month = today.replace(day=1)
last_day_prev_month = first_day_current_month - timedelta(days=1)
target_month = last_day_prev_month.strftime('%Y-%m')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) CTI-Tracker-Bot'}

def fetch_nydfs():
    payload = []
    # Target: NYDFS Industry Letters
    url = "https://www.dfs.ny.gov/industry_guidance/industry_letters"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # NOTE: CSS Selectors must be updated if NYDFS changes website layout
        articles = soup.find_all('div', class_='views-row')
        
        for article in articles:
            date_str = article.find('time').text.strip() if article.find('time') else ""
            try:
                date_obj = datetime.strptime(date_str, '%B %d, %Y')
                if date_obj.strftime('%Y-%m') == target_month:
                    title = article.find('a').text.strip()
                    payload.append({
                        "id": f"NYDFS-{uuid.uuid4().hex[:6]}",
                        "date": date_obj.strftime('%Y-%m-%d'),
                        "agency": "NYDFS",
                        "type": "Guidance/Circular",
                        "title": title,
                        "summary": "Industry Letter published by NYDFS. Review source for full text."
                    })
            except ValueError:
                continue
    except Exception as e:
        print(f"NYDFS Error: {e}")
    return payload

def fetch_finra():
    payload = []
    # Target: FINRA Regulatory Notices via RSS
    url = "https://www.finra.org/rss/finra-notices"
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            # RSS Date Format: 'Tue, 10 Mar 2026 14:00:00 EST'
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
    except Exception as e:
        print(f"FINRA Error: {e}")
    return payload

def fetch_nfa():
    payload = []
    # Target: NFA Notices to Members
    url = "https://www.nfa.futures.org/news/newsNotice.asp"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        rows = soup.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 2:
                date_str = cols[0].text.strip()
                try:
                    date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                    if date_obj.strftime('%Y-%m') == target_month:
                        title = cols[1].text.strip()
                        payload.append({
                            "id": f"NFA-{uuid.uuid4().hex[:6]}",
                            "date": date_obj.strftime('%Y-%m-%d'),
                            "agency": "NFA",
                            "type": "Guidance/Circular",
                            "title": title,
                            "summary": "NFA Notice to Members published. Review source for full text."
                        })
                except ValueError:
                    continue
    except Exception as e:
        print(f"NFA Error: {e}")
    return payload

if __name__ == "__main__":
    tier2_data = []
    tier2_data.extend(fetch_nydfs())
    tier2_data.extend(fetch_finra())
    tier2_data.extend(fetch_nfa())
    
    with open('latest_tier2_pull.json', 'w') as outfile:
        json.dump(tier2_data, outfile, indent=2)
