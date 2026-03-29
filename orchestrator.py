import os
import csv
import json
import google.generativeai as genai

# INITIALIZE AI CONTEXT
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("WARNING: GEMINI_API_KEY not found in environment variables.")

def run_orchestrator(input_file, output_file):
    """
    Reads a raw CSV, maps headers to Shopify format using Gemini AI (JSON mode), 
    and saves the synchronized version.
    """
    model = genai.GenerativeModel('gemini-2.5-flash')
    
    raw_data = []
    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader)
            # Take a sample of the first 5 rows to help the AI understand the data
            sample_rows = []
            for i, row in enumerate(reader):
                sample_rows.append(row)
                if i >= 4: break
            
            # Reset and read full data for the actual transformation
            f.seek(0)
            next(reader)
            raw_data = list(reader)

        shopify_headers = [
            "Handle", "Title", "Body (HTML)", "Vendor", "Type", "Tags", "Published",
            "Option1 Name", "Option1 Value", "Variant SKU", "Variant Grams", "Variant Inventory Tracker",
            "Variant Inventory Qty", "Variant Inventory Policy", "Variant Fulfillment Service",
            "Variant Price", "Variant Compare At Price", "Image Src"
        ]

        # AI PROMPT: TEACHING THE MAPPING (STRICT JSON)
        prompt = f"Map these input headers to the target headers based on the sample data.\n" \
                 f"Input headers: {headers}\n" \
                 f"Sample data: {sample_rows}\n" \
                 f"Target headers: {shopify_headers}\n\n" \
                 f"Return a strict JSON object where the keys are the exact Target headers, and the values are the integer index (0-based) of the matching Input header. " \
                 f"If there is no match for a target header, use null as the value. Do not write any other text."

        # GENERATE MAPPING
        response = model.generate_content(prompt)
        
        # PARSE AI JSON RESPONSE SAFELY
        json_text = response.text.strip()
        # Clean markdown formatting if the AI wrapped it in code blocks
        if json_text.startswith("```json"):
            json_text = json_text[7:]
        elif json_text.startswith("```"):
            json_text = json_text[3:]
            
        if json_text.endswith("```"):
            json_text = json_text[:-3]
            
        mapping_dict = json.loads(json_text.strip())

        # TRANSFORM AND SAVE
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(shopify_headers)
            
            for row in raw_data:
                new_row = []
                for target_header in shopify_headers:
                    idx = mapping_dict.get(target_header)
                    # Only insert data if the AI successfully mapped an integer and it's within bounds
                    if isinstance(idx, int) and 0 <= idx < len(row):
                        new_row.append(row[idx])
                    else:
                        new_row.append("")
                        
                writer.writerow(new_row)
                
        return True
    except Exception as e:
        print(f"Orchestrator Error: {str(e)}")
        return False
