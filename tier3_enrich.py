import json
import os
import requests
from bs4 import BeautifulSoup
from google import genai

api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    print("Fatal: GEMINI_API_KEY environment variable is required.")
    exit(1)

client = genai.Client(api_key=api_key)

def extract_text(url: str) -> str:
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for script in soup(["script", "style"]):
            script.extract()
            
        text = soup.get_text(separator=' ', strip=True)
        return text[:30000]
    except Exception as e:
        print(f"Extraction failed for {url}: {e}")
        return ""

def generate_intelligence(text: str) -> str:
    if not text:
        return "Source extraction failed. Manual review required."
        
    prompt = f"""
    Analyze the following regulatory document. Generate a strict, 3-sentence intelligence summary focusing on:
    1. The core mandate or action taken.
    2. The operational impact on financial institutions.
    3. Required compliance deadlines or technical changes.
    Output only the summary text. No introductory filler.
    
    Document Text:
    {text}
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
        )
        return response.text.strip()
    except Exception as e:
        print(f"LLM inference failed: {e}")
        return "LLM processing error."

def execute_tier3():
    file_path = 'master_data.json'
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Failed to read data: {e}")
        return
        
    modified = False
    for item in data:
        current_summary = item.get("summary", "")
        
        # GATEKEEPER: Bypass records that already possess valid summaries
        if current_summary and "No summary provided" not in current_summary:
            continue
            
        url = item.get("source_url", "")
        if not url:
            continue
            
        print(f"Enriching: {item.get('id')} - {item.get('title')[:40]}...")
        raw_text = extract_text(url)
        new_summary = generate_intelligence(raw_text)
        
        # FAIL-SAFE: Prevent JSON payload corruption via error strings
        if "Manual review required" not in new_summary and "LLM processing error" not in new_summary:
            item["summary"] = new_summary
            modified = True
            
    if modified:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        print("Tier 3 enrichment complete. master_data.json updated.")
    else:
        print("No eligible records found for enrichment.")

if __name__ == "__main__":
    execute_tier3()