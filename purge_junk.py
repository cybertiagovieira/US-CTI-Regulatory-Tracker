import json

def purge_artifacts():
    file_path = 'master_data.json'
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Ledger initialization failed: {e}.")
        return

    initial_count = len(data)
    
    # 1. Drop bad SRO artifact pattern
    clean_data = [
        item for item in data 
        if not (item.get("agency") == "Other/SRO" and 
                item.get("type") == "Guidance/Circular" and 
                item.get("summary", "").strip() == "")
    ]

    # 2. Duplicate URL stripping logic correctly implemented
    seen_urls = set()
    deduped_data = []
    for item in clean_data:
        url = item.get("source_url")
        # Require URL to be truthy and longer than 5 chars to qualify for deduplication. Prevents deleting manual entries.
        if url and len(url) > 5:
            if url in seen_urls:
                continue
            seen_urls.add(url)
        deduped_data.append(item)

    removed_count = initial_count - len(deduped_data)

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(deduped_data, f, indent=2)
    except Exception as e:
        print(f"Write failure: {e}")
        return

    print(f"Purge complete. Terminated {removed_count} artifacts. {len(deduped_data)} records retained.")

if __name__ == "__main__":
    purge_artifacts()