import json
from difflib import SequenceMatcher

DATA_FILE = 'master_data.json'
SIMILARITY_THRESHOLD = 0.80

def calculate_similarity(a, b):
    if not a or not b: 
        return 0.0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()

def execute_purge():
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        print("Fatal: master_data.json unreadable or missing.")
        return

    retained = []
    purged = 0

    for new_item in data:
        is_duplicate = False
        for existing_item in retained:
            # Baseline Constraint: Exact URL collision
            url_a = new_item.get('source_url')
            url_b = existing_item.get('source_url')
            if url_a and url_b and url_a == url_b:
                is_duplicate = True
                break

            # Semantic Constraint: Agency + Date + Title topology
            if new_item.get('agency') == existing_item.get('agency') and new_item.get('date') == existing_item.get('date'):
                sim = calculate_similarity(new_item.get('title'), existing_item.get('title'))
                if sim >= SIMILARITY_THRESHOLD:
                    is_duplicate = True
                    break

        if is_duplicate:
            purged += 1
        else:
            retained.append(new_item)

    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(retained, f, indent=2)

    print(f"Purge complete. Semantic duplicates removed: {purged}. Total records retained: {len(retained)}")

if __name__ == "__main__":
    execute_purge()
