import hashlib
import json
import os

DB_FILE = "mock_fingerprint_db.json"

def init_db():
    """Creates the mock database JSON file if it doesn't exist."""
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, 'w') as f:
            json.dump({}, f)

def generate_fingerprint(headers_list):
    """
    Takes a list of headers, normalizes them (lowercase, strips spaces),
    and generates a strict SHA-256 cryptographic hash.
    """
    # Normalize: [' Part number ', 'EAN13'] -> 'partnumber,ean13'
    # This prevents accidental mismatches due to random trailing spaces
    normalized = ",".join([str(h).strip().lower().replace(" ", "") for h in headers_list])
    
    # Generate Cryptographic Hash
    hash_object = hashlib.sha256(normalized.encode('utf-8'))
    return hash_object.hexdigest()

def lookup_fingerprint(header_hash):
    """Checks if the hash exists in our database."""
    init_db()
    with open(DB_FILE, 'r') as f:
        db = json.load(f)
        
    if header_hash in db:
        print(f"> [MATCH FOUND] Bypassing AI. Loading saved script: {db[header_hash]['script_file']}")
        return db[header_hash]['script_file']
    else:
        print(f"> [NEW SUPPLIER DETECTED] No match found for hash {header_hash[:8]}...")
        return None

def save_fingerprint(header_hash, script_file, supplier_name="Unknown"):
    """Saves a new fingerprint to the database after the AI generates the script."""
    init_db()
    with open(DB_FILE, 'r') as f:
        db = json.load(f)
        
    db[header_hash] = {
        "supplier": supplier_name,
        "script_file": script_file
    }
    
    with open(DB_FILE, 'w') as f:
        json.dump(db, f, indent=4)
    print(f"> [SAVED] Fingerprint locked in for {supplier_name}.")

if __name__ == "__main__":
    # --- ISOLATED TEST HARNESS ---
    print("\n=== PHASE 1: FIRST UPLOAD ===")
    
    # These are the exact true headers from your price-list-n-30 file
    sample_headers = [
        'Part number', 'EAN13', 'Description', 'Quantity per box', 
        'Retail price', 'Currency code', 'Gross weight per box', 
        'Net weight per piece', 'Volume', 'Family'
    ]
    
    # 1. Generate the hash
    my_hash = generate_fingerprint(sample_headers)
    print(f"> Generated SHA-256 Hash: {my_hash}")
    
    # 2. Lookup (Will fail the first time because the DB is empty)
    script = lookup_fingerprint(my_hash)
    
    # 3. Save the pipeline we built in the previous step
    if not script:
        print("> Simulating AI generating the script (Cost: $0.05)...")
        save_fingerprint(my_hash, "generated_pipeline.py", "Euro-Plumbing Supplier")
        
    print("\n=== PHASE 2: NEXT TUESDAY (NEW FILE, SAME SUPPLIER) ===")
    
    # 4. Lookup again (Will succeed and load the script we just saved)
    script_second_time = lookup_fingerprint(my_hash)
    if script_second_time:
        print(f"> Running {script_second_time} in microVM (Cost: $0.00).")