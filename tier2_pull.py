from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import feedparser
from datetime import datetime, timedelta
import json
import uuid

target_month = (datetime.today().replace(day=1) - timedelta(days=1)).strftime('%Y-%m')

def fetch_rendered_html(url):
    html = ""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(4000) # Buffer to allow WAF JS challenge execution
            html = page.content()
        except Exception as e:
            print(f"Error fetching {url}: {e}")
        finally:
            browser.close()
    return html

def fetch_nydfs():
    payload = []
    print("Executing NYDFS DOM Extraction...")
    html = fetch_rendered_html("https://www.dfs.ny.gov/industry_guidance/industry_letters")
    soup = BeautifulSoup(html, 'html.parser')
    articles = soup.find_all('div', class_='views-row')
    
    for article in articles:
        time_tag = article.find('time')
        if not time_tag: continue
        try:
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
        except ValueError: continue
    return payload

def fetch_finra():
    payload = []
    print("Executing FINRA DOM Extraction...")
    html = fetch_rendered_html("https://www.finra.org/rss/finra-notices")
    feed = feedparser.parse(html)
    
    for entry in feed.entries:
        try:
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
        except Exception: continue
    return payload

def fetch_nfa():
    payload = []
    print("Executing NFA DOM Extraction...")
    html = fetch_rendered_html("https://www.nfa.futures.org/news/newsNotice.asp")
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all('tr')
    
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
    return payload

if __name__ == "__main__":
    tier2_data = []
    tier2_data.extend(fetch_nydfs())
    tier2_data.extend(fetch_finra())
    tier2_data.extend(fetch_nfa())
    
    print(f"Total Tier 2 Records Extracted: {len(tier2_data)}")
    with open('latest_tier2_pull.json', 'w') as outfile:
        json.dump(tier2_data, outfile, indent=2)
