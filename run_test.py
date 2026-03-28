from price_list_30_mapper import transform_supplier_data

input_file = "price-list-n-30-ean-excel.xlsx"
output_file = "shopify_ready_output.csv"

print(f"Sending {input_file} to the Builder Robot...")
success = transform_supplier_data(input_file, output_file)

if success:
    print(f"Success! The clean Shopify file was created: {output_file}")
else:
    print("The robot crashed.")