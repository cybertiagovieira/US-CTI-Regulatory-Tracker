import json

def purge_artifacts():
    file_path = 'master_data.json'
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Read failure: {e}")
        return

    initial_count = len(data)
    
    # Isolate and retain valid records. Drop specific artifact pattern.
    clean_data = [
        item for item in data 
        if not (item.get("agency") == "Other/SRO" and 
                item.get("type") == "Guidance/Circular" and 
                item.get("summary", "").strip() == "")
    ]

    removed_count = initial_count - len(clean_data)

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(clean_data, f, indent=2)
    except Exception as e:
        print(f"Write failure: {e}")
        return

    print(f"Purge complete. Terminated {removed_count} artifacts. {len(clean_data)} records retained.")

# purge_junk.py modification
url = item.get("source_url")
# Require URL to be truthy and longer than 5 chars to qualify for deduplication
if url and len(url) > 5:
    if url in seen_urls:
        print(f"Dropping duplicate URL: {url}")
        continue
    seen_urls.add(url)    

try:
    with open(FILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
except (FileNotFoundError, json.JSONDecodeError) as e:
    print(f"Ledger initialization failed: {e}. Defaulting to empty array.")
    data = []

if __name__ == "__main__":
    purge_artifacts()
