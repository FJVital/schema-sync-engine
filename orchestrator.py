import os
import csv
import google.generativeai as genai

# INITIALIZE AI CONTEXT
# We pull the key directly from Render's Environment Variables
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    print("WARNING: GEMINI_API_KEY not found in environment variables.")

def run_orchestrator(input_file, output_file):
    """
    Reads a raw CSV, maps headers to Shopify format using Gemini AI, 
    and saves the synchronized version.
    """
    model = genai.GenerativeModel('gemini-pro')
    
    raw_data = []
    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = next(reader)
            # Take a sample of the first 5 rows to help the AI understand the data
            sample_rows = []
            for i, row in enumerate(reader):
                sample_rows.append(row)
                if i > 5: break
            
            # Reset and read full data for the actual transformation
            f.seek(0)
            next(reader)
            raw_data = list(reader)

        # AI PROMPT: TEACHING THE MAPPING
        prompt = f"Map these headers: {headers}. Sample data: {sample_rows}. " \
                 f"Target headers: Handle, Title, Body (HTML), Vendor, Type, Tags, Published, " \
                 f"Option1 Name, Option1 Value, Variant SKU, Variant Grams, Variant Inventory Tracker, " \
                 f"Variant Inventory Qty, Variant Inventory Policy, Variant Fulfillment Service, " \
                 f"Variant Price, Variant Compare At Price, Image Src. " \
                 f"Return ONLY a comma-separated list of the indices from the input that match these target headers."

        # GENERATE MAPPING
        response = model.generate_content(prompt)
        mapping_indices = response.text.strip().split(',')

        # TRANSFORM AND SAVE
        shopify_headers = [
            "Handle", "Title", "Body (HTML)", "Vendor", "Type", "Tags", "Published",
            "Option1 Name", "Option1 Value", "Variant SKU", "Variant Grams", "Variant Inventory Tracker",
            "Variant Inventory Qty", "Variant Inventory Policy", "Variant Fulfillment Service",
            "Variant Price", "Variant Compare At Price", "Image Src"
        ]

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(shopify_headers)
            
            for row in raw_data:
                new_row = []
                for idx in mapping_indices:
                    try:
                        clean_idx = int(idx.strip())
                        if 0 <= clean_idx < len(row):
                            new_row.append(row[clean_idx])
                        else:
                            new_row.append("")
                    except:
                        new_row.append("")
                
                # Fill remaining columns if mapping returned fewer indices than headers
                while len(new_row) < len(shopify_headers):
                    new_row.append("")
                    
                writer.writerow(new_row[:len(shopify_headers)])
                
        return True
    except Exception as e:
        print(f"Orchestrator Error: {str(e)}")
        return False