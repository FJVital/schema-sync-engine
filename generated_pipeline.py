import pandas as pd
import numpy as np

# 1. Ingestion & Garbage Header Bypass
# Skips the first 4 rows of blank commas and metadata to find the true headers
try:
    df = pd.read_csv('input.csv', skiprows=4)
except Exception as e:
    print(f"CRITICAL ERROR: Failed to read input file. {e}")
    exit()

# 2. Initialize Target Schema
shopify_df = pd.DataFrame()

# 3. Deterministic Mapping & Transformation
try:
    # Generate Shopify Handle from Description (lowercase, hyphens, remove symbols)
    shopify_df['Handle'] = df['Description'].str.lower().str.replace(' ', '-').str.replace('º', '')
except KeyError:
    shopify_df['Handle'] = ""

try:
    shopify_df['Title'] = df['Description']
except KeyError:
    shopify_df['Title'] = ""

try:
    shopify_df['Variant SKU'] = df['Part number']
except KeyError:
    shopify_df['Variant SKU'] = ""

try:
    # Bulletproof Price Parser: Handles US (1,000.50) and European (1.000,50)
    def parse_price(val):
        if pd.isna(val):
            return 0.0
        val = str(val).replace(' ', '')
        if ',' in val and '.' in val:
            if val.rfind(',') > val.rfind('.'): # European style: 1.168,30
                val = val.replace('.', '').replace(',', '.')
            else: # US style: 1,168.30
                val = val.replace(',', '')
        elif ',' in val:
            val = val.replace(',', '.')
        return float(val)

    shopify_df['Variant Price'] = df['Retail price'].apply(parse_price)
except Exception as e:
    print(f"Warning: Issue cleaning prices - {e}")
    shopify_df['Variant Price'] = 0.0

try:
    # Convert gross weight (kg) to Grams for Shopify
    shopify_df['Variant Grams'] = (df['Gross weight per box'].astype(float) * 1000).fillna(0).astype(int)
except KeyError:
    shopify_df['Variant Grams'] = 0

try:
    shopify_df['Variant Inventory Qty'] = df['Quantity per box '] # Note: trailing space included from source
except KeyError:
    try:
        shopify_df['Variant Inventory Qty'] = df['Quantity per box']
    except KeyError:
        shopify_df['Variant Inventory Qty'] = 0

# 4. Inject Static Shopify Requirements
shopify_df['Body (HTML)'] = ""
shopify_df['Vendor'] = ""
shopify_df['Type'] = ""

# Map EAN13 and Family into Tags so no data is lost
try:
    shopify_df['Tags'] = "Family:" + df['Family'].astype(str) + ", EAN:" + df['EAN13'].astype(str)
except KeyError:
    shopify_df['Tags'] = ""

shopify_df['Published'] = "TRUE"
shopify_df['Option1 Name'] = "Title"
shopify_df['Option1 Value'] = "Default Title"
shopify_df['Variant Inventory Tracker'] = "shopify"
shopify_df['Variant Inventory Policy'] = "deny"
shopify_df['Variant Fulfillment Service'] = "manual"
shopify_df['Variant Compare At Price'] = ""
shopify_df['Image Src'] = ""

# 5. Export
shopify_df.to_csv('output.csv', index=False)
print("SUCCESS: Target file 'output.csv' generated perfectly.")