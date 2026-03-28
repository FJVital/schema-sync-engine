import os
from google import genai

if "GOOGLE_API_KEY" in os.environ:
    del os.environ["GOOGLE_API_KEY"]

api_key = os.environ.get("GEMINI_API_KEY")

print(f"\n=== DIAGNOSTIC PROBE ===")
print("Contacting Google Servers to list your permitted models...\n")

try:
    client = genai.Client(api_key=api_key)
    available_models = client.models.list()
    
    count = 0
    for model in available_models:
        print(f" [AUTHORIZED] {model.name}")
        count += 1
            
    print(f"\n> Total models available: {count}")
    print("=== PROBE COMPLETE ===\n")

except Exception as e:
    print(f"\n[CRITICAL ERROR] {e}")
