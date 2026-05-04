import os
import json
import time
import requests
from bs4 import BeautifulSoup
import google.generativeai as genai

DATA_FILE = 'master_data.json'

def smart_truncate(text, max_chars=80000):
    """
    Executes bimodal truncation to preserve critical start/end metadata.
    max_chars=80000 yields ~20,000 tokens.
    Allocates 40% to the Head (Summary) and 60% to the Tail (Deadlines/Mandates).
    """
    if not text or len(text) <= max_chars:
        return text

    head_limit = int(max_chars * 0.4)
    tail_limit = int(max_chars * 0.6)

    head_text = text[:head_limit]
    tail_text = text[-tail_limit:]
    
    redaction_warning = "\n\n...[SYSTEM REDACTION: MIDDLE SECTION OMITTED FOR TOKEN OPTIMIZATION]...\n\n"

    return head_text + redaction_warning + tail_text

def fetch_document_text(url):
    """Scrapes raw text from the source URL if not provided by Tier 1/2."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        # Strip script and style elements
        for script in soup(["script", "style"]):
            script.extract()
        return soup.get_text(separator=' ', strip=True)
    except Exception as e:
        print(f"Extraction failed for {url}: {e}")
        return ""

def process_tier3():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Fatal: GEMINI_API_KEY environment variable is required.")
        exit(1)

    genai.configure(api_key=api_key)
    
    # gemini-1.5-flash is optimized for large-context regulatory ingestion
    model = genai.GenerativeModel('gemini-1.5-flash')

    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Fatal: {DATA_FILE} unreadable or missing.")
        return

    updated_count = 0

    for item in data:
        # Bypass if already fully enriched
        if item.get('summary') and item.get('theme') and item.get('severity'):
            continue

        print(f"Enriching ID {item.get('id', 'UNKNOWN')}...")
        
        document_text = item.get('raw_text', '')
        if not document_text and item.get('source_url'):
            document_text = fetch_document_text(item['source_url'])

        if not document_text:
            print(f"Skipping ID {item.get('id')}: No document text available.")
            continue

        # Execute Bimodal Truncation
        raw_text = smart_truncate(document_text, max_chars=80000)

        prompt = f"""
        Analyze the following US cyber regulatory document and extract key intelligence.
        Respond ONLY with a valid JSON object matching this exact schema. Do not use markdown blocks.

        {{
            "summary": "A 2-3 sentence executive intelligence summary.",
            "severity": "Low", "Medium", or "High",
            "theme": "General", "Incident Reporting", "Ransomware", "Third-Party Risk", "Data Privacy", or "Governance",
            "target_sector": "All Entities", "G-SIBs", "Broker-Dealers", "Credit Unions", or "Digital Asset Platforms",
            "effective_date": "YYYY-MM-DD" (or null if not found)
        }}

        Document Text:
        {raw_text}
        """

        try:
            response = model.generate_content(prompt)
            
            # Sanitize LLM payload
            response_text = response.text.replace('```json', '').replace('```', '').strip()
            extracted_data = json.loads(response_text)

            item['summary'] = extracted_data.get('summary', item.get('summary'))
            item['severity'] = extracted_data.get('severity', item.get('severity'))
            item['theme'] = extracted_data.get('theme', item.get('theme'))
            item['target_sector'] = extracted_data.get('target_sector', item.get('target_sector'))
            
            if extracted_data.get('effective_date'):
                item['effective_date'] = extracted_data.get('effective_date')

            # Purge raw text to maintain lightweight JSON payload for client frontend
            if 'raw_text' in item:
                del item['raw_text']

            updated_count += 1
            time.sleep(2) # Rate limit protection

        except Exception as e:
            print(f"Inference failed for ID {item.get('id')}: {e}")

    if updated_count > 0:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        print(f"Tier 3 process complete. Records enriched: {updated_count}")
    else:
        print("Tier 3 process complete. No enrichment required.")

if __name__ == "__main__":
    process_tier3()
