import pandas as pd

# Define the exact functions as specified in Rule 5
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

# Target Shopify Schema (Rule 6)
SHOPIFY_COLUMNS = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Type",
    "Tags",
    "Published",
    "Option1 Name",
    "Option1 Value",
    "Variant SKU",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Qty",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "Variant Price",
    "Variant Compare At Price",
    "Image Src"
]

# Read the source CSV (Rule 2)
# Rule 3: Analyze sample for garbage header. The sample provided starts directly with column names,
# so skiprows=0 (or omitting skiprows) is appropriate.
try:
    df_source = pd.read_csv('input.csv', skiprows=0)
except Exception as e:
    # Defensive coding for file reading errors
    print(f"Error reading input.csv: {e}")
    # Create an empty DataFrame with expected source columns to prevent further errors
    df_source = pd.DataFrame(columns=[
        "id", "sku", "title", "short description", "description",
        "category", "link", "image_link", "price", "shipping",
        "stock", "Fitment"
    ])

# Initialize the target DataFrame with the correct Shopify columns (Rule 6)
df_shopify = pd.DataFrame(index=df_source.index, columns=SHOPIFY_COLUMNS)

# Populate Shopify columns with transformations and defensive coding (Rule 4, 6)

# Handle
try:
    if 'title' in df_source.columns:
        df_shopify['Handle'] = df_source['title'].astype(str).str.lower().str.replace('[^a-z0-9]+', '-', regex=True).str.strip('-')
    else:
        df_shopify['Handle'] = ""
except Exception as e:
    df_shopify['Handle'] = ""
    # print(f"Error processing 'Handle': {e}") # Suppress print for pure code output

# Title
try:
    if 'title' in df_source.columns:
        df_shopify['Title'] = df_source['title'].astype(str)
    else:
        df_shopify['Title'] = ""
except Exception as e:
    df_shopify['Title'] = ""
    # print(f"Error processing 'Title': {e}")

# Body (HTML)
try:
    if 'description' in df_source.columns:
        df_shopify['Body (HTML)'] = df_source['description'].astype(str)
    else:
        df_shopify['Body (HTML)'] = ""
except Exception as e:
    df_shopify['Body (HTML)'] = ""
    # print(f"Error processing 'Body (HTML)': {e}")

# Vendor (Missing in source, fill with empty string per Rule 6)
df_shopify['Vendor'] = ""

# Type
try:
    if 'category' in df_source.columns:
        df_shopify['Type'] = df_source['category'].astype(str)
    else:
        df_shopify['Type'] = ""
except Exception as e:
    df_shopify['Type'] = ""
    # print(f"Error processing 'Type': {e}")

# Tags
try:
    if 'category' in df_source.columns:
        # Example: 'Stealth LED Lights > Integration Kits' becomes 'Stealth LED Lights, Integration Kits'
        df_shopify['Tags'] = df_source['category'].astype(str).str.replace(' > ', ', ')
    else:
        df_shopify['Tags'] = ""
except Exception as e:
    df_shopify['Tags'] = ""
    # print(f"Error processing 'Tags': {e}")

# Published (Missing in source, default to "TRUE")
df_shopify['Published'] = "TRUE"

# Option1 Name (Missing in source, default to "Title" for single variant products)
df_shopify['Option1 Name'] = "Title"

# Option1 Value
try:
    if 'title' in df_source.columns:
        df_shopify['Option1 Value'] = df_source['title'].astype(str)
    else:
        df_shopify['Option1 Value'] = ""
except Exception as e:
    df_shopify['Option1 Value'] = ""
    # print(f"Error processing 'Option1 Value': {e}")

# Variant SKU
try:
    if 'sku' in df_source.columns:
        df_shopify['Variant SKU'] = df_source['sku'].astype(str)
    else:
        df_shopify['Variant SKU'] = ""
except Exception as e:
    df_shopify['Variant SKU'] = ""
    # print(f"Error processing 'Variant SKU': {e}")

# Variant Grams (Missing in source, default to 0 per Rule 6)
df_shopify['Variant Grams'] = 0

# Variant Inventory Tracker (Missing in source, default to "shopify")
df_shopify['Variant Inventory Tracker'] = "shopify"

# Variant Inventory Qty (Rule 5: Apply parse_qty)
try:
    if 'stock' in df_source.columns:
        df_shopify['Variant Inventory Qty'] = df_source['stock'].apply(parse_qty)
    else:
        df_shopify['Variant Inventory Qty'] = 0 # Default if column missing
except Exception as e:
    df_shopify['Variant Inventory Qty'] = 0
    # print(f"Error processing 'Variant Inventory Qty': {e}")

# Variant Inventory Policy (Missing in source, default to "deny")
df_shopify['Variant Inventory Policy'] = "deny"

# Variant Fulfillment Service (Missing in source, default to "manual")
df_shopify['Variant Fulfillment Service'] = "manual"

# Variant Price (Rule 5: Apply parse_price)
try:
    if 'price' in df_source.columns:
        df_shopify['Variant Price'] = df_source['price'].apply(parse_price)
    else:
        df_shopify['Variant Price'] = 0.0 # Default if column missing
except Exception as e:
    df_shopify['Variant Price'] = 0.0
    # print(f"Error processing 'Variant Price': {e}")

# Variant Compare At Price (Missing in source, fill with empty string per Rule 6)
df_shopify['Variant Compare At Price'] = ""

# Image Src
try:
    if 'image_link' in df_source.columns:
        df_shopify['Image Src'] = df_source['image_link'].astype(str)
    else:
        df_shopify['Image Src'] = ""
except Exception as e:
    df_shopify['Image Src'] = ""
    # print(f"Error processing 'Image Src': {e}")

# Ensure all target columns exist and are in the correct order (Rule 6)
for col in SHOPIFY_COLUMNS:
    if col not in df_shopify.columns:
        df_shopify[col] = "" # Fill with empty string for newly added missing columns

df_shopify = df_shopify[SHOPIFY_COLUMNS]

# Export to output.csv (Rule 2)
try:
    df_shopify.to_csv('output.csv', index=False)
except Exception as e:
    print(f"Error writing to output.csv: {e}")