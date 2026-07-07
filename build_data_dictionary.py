import os
import csv

# This automatically finds your files based on where the script is located
TARGET_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(TARGET_DIR, "market_data_dictionary.md")

def identify_type(sample_val, col_name):
    if not sample_val or str(sample_val).strip() == "":
        return "Empty / Null Data"
    val = str(sample_val).strip()
    name_lower = col_name.lower()
    if any(k in name_lower for k in ["date", "time", "expiry", "timestamp"]):
        return "📅 Standardised Date / Time"
    if any(k in name_lower for k in ["symbol", "ticker", "instrument", "option_type", "cp", "right"]):
        return "🔤 Text / Categorical Key"
    try:
        int(val)
        return "🔢 Integer (Volume / OI)"
    except ValueError:
        pass
    try:
        float(val)
        return "💵 Float (Price / Metric)"
    except ValueError:
        pass
    return "🔤 Text / Categorical Key"

md_content = "# 📊 MarketForge Master Data Dictionary\n\n"
md_content += f"Generated dynamically from: `{TARGET_DIR}`\n\n---\n\n"

print("="*70)
print(f"🚀 SCANNING WORKSPACE: {TARGET_DIR}")
print("="*70)

total_files = 0

for root, dirs, files in os.walk(TARGET_DIR):
    if any(k in root for k in ["_state", ".git", "__pycache__"]):
        continue
        
    target_files = [f for f in files if f.lower().endswith(('.csv', '.parquet'))]
    if not target_files:
        continue
        
    total_files += len(target_files)
    rel_folder = os.path.relpath(root, TARGET_DIR)
    display_folder = "Root Workspace" if rel_folder == "." else rel_folder
    
    print(f" -> Found branch: {display_folder} ({len(target_files)} files)")
    md_content += f"## 📁 Directory Branch: `{display_folder}`\n"
    md_content += f"Total Data Objects Tracked: **{len(target_files)} files**\n\n"
    
    target_files.sort()
    # FIXED: Added [0] to select the first file name instead of the entire list array
    sample_file = target_files[0]
    sample_path = os.path.join(root, sample_file)
    
    try:
        if sample_file.lower().endswith('.csv'):
            with open(sample_path, mode='r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                first_row = next(reader, None)
                
                if headers:
                    md_content += f"### 🛠️ Table Schema Model (Sample Object: `{sample_file}`):\n"
                    md_content += "| Field | Detected Type | Sample Value |\n"
                    md_content += "| :--- | :--- | :--- |\n"
                    for idx, col in enumerate(headers):
                        sample_metric = first_row[idx] if (first_row and idx < len(first_row)) else "N/A"
                        md_content += f"| **{col}** | {identify_type(sample_metric, col)} | `{sample_metric}` |\n"
                    md_content += "\n"
        elif sample_file.lower().endswith('.parquet'):
            md_content += f"### 🛠️ Table Schema Model (Sample Object: `{sample_file}`):\n"
            md_content += "*[⚡ High-Speed Binary Parquet Grid Matrix]*\n\n"
    except Exception as e:
         md_content += f"❌ *Error reading sample: {str(e)}*\n\n"
         
    md_content += f"<details><summary>📦 Click to view inventory (Top 5)</summary>\n\n"
    for f in target_files[:5]:
        md_content += f"- `{f}`\n"
    md_content += "</details>\n\n---\n\n"

with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
    out.write(md_content)

print("="*70)
print(f"✅ COMPLETE! Processed {total_files} files.")
print(f"👉 File saved at: {OUTPUT_FILE}")
print("="*70)
