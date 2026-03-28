import os
import subprocess

# 1. THE NUKE: Destroy the phantom key before Google's SDK can find it
if "GOOGLE_API_KEY" in os.environ:
    del os.environ["GOOGLE_API_KEY"]

from google import genai

# 2. Configuration & API Setup
api_key = os.environ.get("GEMINI_API_KEY")
if not api_key:
    raise ValueError("CRITICAL: GEMINI_API_KEY environment variable not set.")

client = genai.Client(api_key=api_key)

def get_raw_sample(filepath, lines=50):
    """Reads the raw text of the file to avoid Pandas crashing on garbage headers."""
    print(f"> Extracting first {lines} raw lines from {filepath}...")
    sample_lines = []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            for i in range(lines):
                try:
                    sample_lines.append(next(f))
                except StopIteration:
                    break
        return "".join(sample_lines)
    except FileNotFoundError:
        raise FileNotFoundError(f"CRITICAL: Could not find {filepath}. Ensure it is in the same folder.")

def get_system_prompt():
    """Loads the hardened system prompt."""
    with open('system_prompt.txt', 'r') as f:
        return f.read()

def generate_pipeline_script(raw_sample, prompt):
    """Sends the sample to the Coder Agent and retrieves the Python script."""
    print("> Querying Coder Agent...")
    
    full_prompt = f"""
    {prompt}
    
    TARGET SCHEMA (Shopify Standard):
    Handle, Title, Body (HTML), Vendor, Type, Tags, Published, Option1 Name, Option1 Value, Variant SKU, Variant Grams, Variant Inventory Tracker, Variant Inventory Qty, Variant Inventory Policy, Variant Fulfillment Service, Variant Price, Variant Compare At Price, Image Src
    
    SOURCE DATA SAMPLE (Raw Text):
    {raw_sample}
    """
    
    # New syntax for calling the active Gemini endpoint
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents=full_prompt,
    )
    script_content = response.text
    
    # Defensive cleanup: Strip markdown blocks if the AI hallucinates them
    script_content = script_content.replace("```python\n", "").replace("```python", "").replace("```\n", "").replace("```", "")
    
    return script_content.strip()

def run_pipeline():
    # File paths
    input_file = "input.csv"
    script_file = "generated_pipeline.py"
    
    # Step 1: Grab raw sample
    raw_sample = get_raw_sample(input_file)
    
    # Step 2: Load Prompt
    sys_prompt = get_system_prompt()
    
    # Step 3: Generate Code
    python_code = generate_pipeline_script(raw_sample, sys_prompt)
    
    # Step 4: Save the code to a sandbox file
    print("> Saving generated pipeline to generated_pipeline.py...")
    with open(script_file, 'w', encoding='utf-8') as f:
        f.write(python_code)
        
    # Step 5: Execute the generated script via subprocess
    print("> Executing isolated pipeline...")
    try:
        result = subprocess.run(['python', script_file], capture_output=True, text=True, check=True)
        print("\n=== PIPELINE SUCCESS ===")
        print(result.stdout)
        print("> Target file 'output.csv' generated successfully.")
    except subprocess.CalledProcessError as e:
        print("\n=== PIPELINE FAILED ===")
        print(e.stderr)

if __name__ == "__main__":
    run_pipeline()