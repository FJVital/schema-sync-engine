import pandas as pd
import re

# Define the price and quantity parsing functions exactly as specified
def parse_price(val):
    if pd.isna(val): return 0.0
    val = str(val).replace(' ', '')
    if ',' in val and '.' in val:
        if val.rfind(',') > val.rfind('.'):
            val = val.replace('.', '').replace(',', '.')
        else:
            val = val.replace(',', '')
    elif ',' in val:
        val = val.replace(',', '.')
    try: return float(val)
    except: return 0.0

def parse_qty(val):
    if pd.isna(val): return 0
    val = str(val).replace(' ', '').replace('.', '').replace(',', '')
    try: return int(val)
    except: return 0

# Target Shopify Schema
TARGET_SCHEMA = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Type", "Tags", "Published",
    "Option1 Name", "Option1 Value", "Variant SKU", "Variant Grams",
    "Variant Inventory Tracker", "Variant Inventory Qty", "Variant Inventory Policy",
    "Variant Fulfillment Service", "Variant Price", "Variant Compare At Price", "Image Src"
]

# --- Main Script Logic ---
# 1. Read the source CSV
# The actual column names start on the 5th row (index 4), so skiprows=4
try:
    df_source = pd.read_csv('input.csv', skiprows=4)
except FileNotFoundError:
    # This block is for generating a dummy input.csv if not found, for local testing
    # In a production environment, this should ideally raise an error or handle
    # the missing file gracefully based on requirements.
    sample_data = """#VALUE!,,,,,,,,,
,,Price list nº 30. March 2025,,,,,,,
,,,,,,,,,
,,,,,,,,,
Part number,EAN13,Description,Quantity per box ,Retail price,Currency code,Gross weight per box,Net weight per piece,Volume,Family
1001001,8433350000000,ELBOW 90º  25,100,1.09,EUR,3.29,0.031,0.008918,1
1001002,8433350000017,ELBOW 90º  32,100,1.56,EUR,6.05,0.0556,0.022435,1
1001003,8433350000024,ELBOW 90º  40,100,2.25,EUR,9.765,0.0919,0.031294,1
1001004,8433350000031,ELBOW 90º  50,100,3.43,EUR,14.98,0.140595,0.06149,1
1001005,8433350000048,ELBOW 90º  63,50,5.13,EUR,13.915,0.25989,0.06149,1
1001006,8433350000055,ELBOW 90º  75,30,9.46,EUR,13.178,0.41719,0.048799,1
1001007,8433350000062,ELBOW 90º  90,18,15.37,EUR,13.167,0.69455,0.04836,1
1001008,8433350000079,ELBOW 90º 110,10,28.62,EUR,11.401,1.0826,0.048799,1
1001009,8433350000086,ELBOW 90º 125,6,42.89,EUR,9.51,1.470783,0.047171,1
1001010,8433350000093,ELBOW 90º 140,6,73.61,EUR,13.68,2.14885,0.063473,1
1001011,8433350000109,ELBOW 90º 160,4,85.9,EUR,12.64,2.931,0.061499,1
1001012,8433350000116,ELBOW 90º 200,2,126.27,EUR,9.996,4.66545,0.04836,1
1001013,8433350000123,PVC 45º ELBOW  25,100,1.52,EUR,2.84,0.026526,0.008918,1
1001014,8433350000130,PVC 45º ELBOW  32,100,2.04,EUR,4.625,0.042983,0.015532,1
1001015,8433350000147,PVC 45º ELBOW  40,110,2.77,EUR,8.455,0.071655,0.031294,1
1001016,8433350000154,PVC 45º ELBOW  50,110,3.74,EUR,13.596,0.117765,0.047171,1
1001017,8433350000161,PVC 45º ELBOW  63,55,5.5,EUR,12.059,0.207585,0.047171,1
1001018,8433350000178,PVC 45º ELBOW  75,33,10.1,EUR,12.52,0.361806,0.04879,1
1001019,8433350000185,PVC 45º ELBOW  90,16,15.66,EUR,10.895,0.640913,0.041172,1
1001020,8433350000192,PVC 45º ELBOW  110,12,26.28,EUR,11.586,0.912133,0.04717,1
1001021,8433350000208,PVC 45º ELBOW  125,6,38.52,EUR,7.755,1.1935,0.036484,1
1001022,8433350000215,PVC 45º ELBOW  140,6,62.36,EUR,13.505,2.097417,0.061499,1
1001023,8433350000222,PVC 45º ELBOW  160,4,75.89,EUR,8.98,2.101175,0.048799,1
1001024,8433350000239,PVC 45º ELBOW  200,2,112.57,EUR,8.509,3.471,0.0446,1
1001025,8433350000246,PVC 90º TEE  25,100,1.37,EUR,4.752,0.04422,0.015532,1
1001026,8433350000253,PVC 90º TEE  32,70,1.99,EUR,5.555,0.07239,0.022435,1
1001027,8433350000260,PVC 90º TEE  40,80,3.28,EUR,9.83,0.11601,0.036816,1
1001028,8433350000277,PVC 90º TEE  50,65,4.62,EUR,13.6187,0.195357,0.06149,1
1001029,8433350000284,PVC 90º TEE  63,38,6.54,EUR,13.471,0.333792,0.063473,1
1001030,8433350000291,PVC 90º TEE  75,20,13.84,EUR,11.833,0.557385,0.047171,1
1001031,8433350000307,PVC 90º TEE  90,12,24.9,EUR,12.115,0.9561,0.047171,1
1001032,8433350000314,PVC 90º TEE  110,10,37.02,EUR,16.425,1.55045,0.061499,1
1001033,8433350000321,PVC 90º TEE  125,7,60.99,EUR,14.7337,2.02,0.06879,1
1001034,8433350000338,PVC 90º TEE  140,4,92.8,EUR,11.935,2.839925,0.048799,1
1001035,8433350000345,PVC 90º TEE  160,3,109.57,EUR,12.284,3.787833,0.06649,1
1001036,8433350000352,PVC 90º TEE  200,2,158.21,EUR,13.77,6.47,0.063473,1
1001037,8433350000369,PVC COUPLING 25,100,1.13,EUR,2.29,0.021026,0.008918,1
1001038,8433350000376,PVC COUPLING 32,80,1.37,EUR,3.086,0.036233,0.008918,1
1001039,8433350000383,PVC COUPLING 40,80,1.98,EUR,5.66,0.064654,0.022435,1
1001040,8433350000390,PVC COUPLING 50,90,2.68,EUR,10.28,0.107856,0.031294,1
1001041,8433350000406,PVC COUPLING 63,60,3.89,EUR,10.09,0.159013,0.036816,1
1001042,8433350000413,PVC COUPLING 75,30,8.06,EUR,8.53,0.265233,0.031294,1
1001043,8433350000420,PVC COUPLING 90,16,10.29,EUR,7.445,0.443131,0.03129,1
1001044,8433350000437,PVC COUPLING 110,12,17.11,EUR,9.153,0.715,0.031294,1
1001045,8433350000444,PVC COUPLING 125,10,26.48,EUR,10.64,1.00908,0.036816,1
"""
    with open('input.csv', 'w', newline='') as f:
        f.write(sample_data)
    df_source = pd.read_csv('input.csv', skiprows=4)

# Strip whitespace from column names for easier access
df_source.columns = df_source.columns.str.strip()

# Initialize target DataFrame
df_target = pd.DataFrame()

# 2. Populate target columns with transformations, wrapped in try/except for defensiveness

# Handle: Slugify the Description
try:
    df_target['Handle'] = df_source['Description'].astype(str).str.lower().str.replace(r'[^a-z0-9]+', '-', regex=True).str.strip('-').fillna('')
except Exception:
    df_target['Handle'] = ""

# Title: Use Description
try:
    df_target['Title'] = df_source['Description'].astype(str).fillna('')
except Exception:
    df_target['Title'] = ""

# Body (HTML): Use Description
try:
    df_target['Body (HTML)'] = df_source['Description'].astype(str).fillna('')
except Exception:
    df_target['Body (HTML)'] = ""

# Vendor: No explicit vendor column, use a default value
try:
    df_target['Vendor'] = "Default Vendor"
except Exception:
    df_target['Vendor'] = ""

# Type: Extract the first word from Description
try:
    df_target['Type'] = df_source['Description'].astype(str).apply(lambda x: x.split(' ')[0] if pd.notna(x) else "").fillna('')
except Exception:
    df_target['Type'] = ""

# Tags: Use Family column
try:
    df_target['Tags'] = df_source['Family'].astype(str).fillna('')
except Exception:
    df_target['Tags'] = ""

# Published: Set to TRUE
try:
    df_target['Published'] = "TRUE"
except Exception:
    df_target['Published'] = ""

# Option1 Name, Option1 Value: Extract size from Description
# e.g., "ELBOW 90º 25" -> 25
def extract_last_number(text):
    if pd.isna(text): return ""
    text = str(text).strip()
    match = re.search(r'(\d+)$', text)
    return match.group(1) if match else ""

try:
    df_target['Option1 Name'] = "Size"
    df_target['Option1 Value'] = df_source['Description'].apply(extract_last_number).fillna('')
except Exception:
    df_target['Option1 Name'] = ""
    df_target['Option1 Value'] = ""

# Variant SKU: Use Part number
try:
    df_target['Variant SKU'] = df_source['Part number'].astype(str).fillna('')
except Exception:
    df_target['Variant SKU'] = ""

# Variant Grams: Convert 'Net weight per piece' (likely kg) to grams (int)
try:
    # Handles potential European decimals for weight too, though sample uses periods.
    # Convert to float, multiply by 1000, then to integer.
    df_target['Variant Grams'] = df_source['Net weight per piece'].astype(str).str.replace(',', '.', regex=False).apply(
        lambda x: int(float(x) * 1000) if pd.notna(x) and re.match(r'^-?\d+(\.\d+)?$', str(x)) else 0
    )
except Exception:
    df_target['Variant Grams'] = 0

# Variant Inventory Tracker: Set to "shopify"
try:
    df_target['Variant Inventory Tracker'] = "shopify"
except Exception:
    df_target['Variant Inventory Tracker'] = ""

# Variant Inventory Qty: Use 'Quantity per box ' and apply parse_qty function
try:
    df_target['Variant Inventory Qty'] = df_source['Quantity per box'].apply(parse_qty)
except Exception:
    df_target['Variant Inventory Qty'] = 0

# Variant Inventory Policy: Set to "deny"
try:
    df_target['Variant Inventory Policy'] = "deny"
except Exception:
    df_target['Variant Inventory Policy'] = ""

# Variant Fulfillment Service: Set to "manual"
try:
    df_target['Variant Fulfillment Service'] = "manual"
except Exception:
    df_target['Variant Fulfillment Service'] = ""

# Variant Price: Use 'Retail price' and apply parse_price function
try:
    df_target['Variant Price'] = df_source['Retail price'].apply(parse_price)
except Exception:
    df_target['Variant Price'] = 0.0

# Variant Compare At Price: Not in source, fill with empty string
try:
    df_target['Variant Compare At Price'] = ""
except Exception:
    df_target['Variant Compare At Price'] = ""

# Image Src: Not in source, fill with empty string
try:
    df_target['Image Src'] = ""
except Exception:
    df_target['Image Src'] = ""

# 3. Ensure all TARGET_SCHEMA columns exist and are in the correct order
# Any columns required by Shopify but not created above will be added here
for col in TARGET_SCHEMA:
    if col not in df_target.columns:
        df_target[col] = "" # Create missing columns with empty strings

df_final = df_target[TARGET_SCHEMA]

# 4. Export the transformed DataFrame to output.csv
df_final.to_csv('output.csv', index=False)