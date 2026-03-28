import os
import csv
import json
import hashlib
import google.generativeai as genai

print("\n=== SCHEMASYNC MASTER ORCHESTRATOR ===")

# 🔴🔴🔴 YOUR GEMINI API KEY GOES HERE 🔴🔴🔴
# Replace the text inside the quotes with your actual AIzaSy... key
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
genai.configure(api_key=GEMINI_API_KEY)

# Initialize the AI Model
model = genai.GenerativeModel('gemini-2.5-flash')

def get_file_hash(filepath, num_lines=5):
    """Generates a unique fingerprint for the supplier CSV format."""
    print(f"> Extracting first {num_lines} raw lines from {filepath}...")
    hasher = hashlib.md5()
    with open(filepath, 'r', encoding='utf-8-sig', errors='ignore') as f:
        for i, line in enumerate(f):
            if i >= num_lines: break
            hasher.update(line.encode('utf-8'))
    return hasher.hexdigest()

def get_ai_mapping(headers, sample_data):
    """Asks Gemini to map the raw headers to Shopify headers."""
    print("> Booting Gemini Coder Agent...")
    print("> Querying Coder Agent for Schema Mapping...")
    
    prompt = f"""
    You are an expert data engineer. Map these raw supplier columns to Shopify CSV columns.
    
    Raw Headers: {headers}
    Sample Data Row: {sample_data}
    
    Standard Shopify Headers Required:
    Handle, Title, Body (HTML), Vendor, Type, Tags, Published, Option1 Name, Option1 Value, Variant SKU, Variant Inventory Qty, Variant Price, Image Src
    
    Return ONLY a valid JSON object where the keys are the Shopify Headers and the values are the EXACT matching Raw Headers. 
    If there is no match for a Shopify header, leave the value as an empty string "". Do not include markdown formatting.
    """
    
    try:
        response = model.generate_content(prompt)
        # Clean up the response in case Gemini adds markdown code blocks
        clean_json = response.text.replace("```json", "").replace("```", "").strip()
        mapping = json.loads(clean_json)
        print("> AI Mapping Successful.")
        return mapping
    except Exception as e:
        print(f"> CRITICAL AI ERROR: {e}")
        raise

def process_file():
    input_file = "input.csv"
    output_file = "output.csv"
    
    if not os.path.exists(input_file):
        print("> ERROR: input.csv not found.")
        return

    file_hash = get_file_hash(input_file)
    print(f"> [NEW SUPPLIER DETECTED] Processing format hash {file_hash[:8]}...")

    # Read the data
    with open(input_file, 'r', encoding='utf-8-sig', errors='ignore') as infile:
        reader = csv.DictReader(infile)
        raw_headers = reader.fieldnames
        
        # Grab a sample row for the AI
        try:
            sample_row = next(reader)
        except StopIteration:
            print("> ERROR: CSV is empty.")
            return

    # Ask AI to generate the mapping rules
    mapping_rules = get_ai_mapping(raw_headers, sample_row)
    shopify_headers = list(mapping_rules.keys())

    print("> Formatting data to Shopify standards...")
    
    # Process the entire file using the AI's rules
    with open(input_file, 'r', encoding='utf-8-sig', errors='ignore') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=shopify_headers)
        writer.writeheader()
        
        # Write the sample row we extracted earlier
        first_row_out = {s_head: sample_row.get(r_head, "") for s_head, r_head in mapping_rules.items()}
        writer.writerow(first_row_out)
        
        # Process the rest of the 2,000+ rows instantly
        for row in reader:
            out_row = {s_head: row.get(r_head, "") for s_head, r_head in mapping_rules.items()}
            writer.writerow(out_row)

    print("> Orchestrator Pipeline Complete. output.csv generated.")

if __name__ == "__main__":
    process_file()
