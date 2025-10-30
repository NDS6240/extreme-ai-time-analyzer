import json
import os
from gmail_fetcher import fetch_reports_from_gmail
from report_parser import parse_report  # Import the new LLM-based parser
from pathlib import Path
from report_parser import ALL_RESULTS
from export_to_excel import export_summary_excel
from google_sheets_updater import update_google_sheets

if __name__ == "__main__":
    print("📩 Fetching new reports from Gmail...")
    fetch_reports_from_gmail()

    print("\n🧠 Analyzing downloaded reports...\n")
    downloads_path = Path("downloads")
    if not downloads_path.exists():
        print("⚠️ No 'downloads' folder found. Please ensure files are downloaded correctly.")
    else:
        all_files_in_dir = list(downloads_path.glob("*")) # Get all items in dir
        if not all_files_in_dir:
            print("⚠️ No files found in 'downloads' folder.")
        else:
            supported_extensions = [".pdf", ".xlsx", ".xls", ".csv"]

            # --- NEW: Pre-filter the list to get an accurate total count ---
            files_to_process = []
            for f in all_files_in_dir:
                # Skip already processed or generated files
                if "_parsed" in f.name or f.name.startswith(".") or f.suffix.lower() not in supported_extensions:
                    print(f"--- Skipping generated or unsupported file: {f.name} ---")
                    continue
                # If it's valid, add it to our list
                files_to_process.append(f)
            
            total_count = len(files_to_process)
            
            if total_count == 0:
                 print("⚠️ No valid report files found to process.")
            # --- END NEW ---


            # --- MODIFIED: Loop over the pre-filtered list ---
            for i, f in enumerate(files_to_process, start=1):
                # (The skip logic is no longer needed here, it's done above)

                # --- MODIFIED PRINT STATEMENT (as requested) ---
                print(f"\n--- Analyzing Report {i}/{total_count}: {f.name} ---")
                
                result = parse_report(str(f))

                # Print detailed results
                print(f"  File: {result['file']}")
                print(f"  Name: {result['employee_name']}")
                print(f"  ID: {result['employee_id']}")
                print(f"  Period: {result['report_period']}")
                summary_str = json.dumps(result.get('report_summary'), indent=4, ensure_ascii=False)
                print(f"  Summary:\n{summary_str}")
                
                # --- (File deletion logic from last time) ---
                try:
                    # *** Per user request: Delete file after processing ***
                    # *** To keep local files, comment out the line below (# os.remove(f)) ***
                    os.remove(f)
                    
                    if (os.getenv("DEBUG", "False").lower() == "true"):
                         print(f"  🗑️ Removed local file: {f.name}")
                except OSError as e:
                    print(f"  ⚠️ Could not remove file {f.name}: {e}")
                # --- END ---

    print("\n✅ Process complete.")

    export_summary_excel(ALL_RESULTS)
    update_google_sheets(ALL_RESULTS)