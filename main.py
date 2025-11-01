"""
Main entry point for the Extreme AI Time Analyzer.
Processes employee attendance reports from Gmail, validates data, and exports to Excel/Google Sheets.
"""
import json
import os
from gmail_fetcher import fetch_reports_from_gmail
from report_parser import parse_report
from pathlib import Path
from report_parser import ALL_RESULTS, EMPLOYEE_NAMES
from export_to_excel import export_summary_excel
from google_sheets_updater import update_google_sheets

if __name__ == "__main__":
    # Step 1: Fetch new reports from Gmail
    print("📩 Fetching new reports from Gmail...")
    fetch_reports_from_gmail()

    # Step 2: Process downloaded reports
    print("\n🧠 Analyzing downloaded reports...\n")
    downloads_path = Path("downloads")
    
    if not downloads_path.exists():
        print("⚠️ No 'downloads' folder found. Please ensure files are downloaded correctly.")
    else:
        all_files_in_dir = list(downloads_path.glob("*"))
        
        if not all_files_in_dir:
            print("⚠️ No files found in 'downloads' folder.")
        else:
            supported_extensions = [".pdf", ".xlsx", ".xls", ".csv"]
            
            # Pre-filter files: exclude already processed or unsupported files
            files_to_process = []
            for f in all_files_in_dir:
                if "_parsed" in f.name or f.name.startswith(".") or f.suffix.lower() not in supported_extensions:
                    print(f"--- Skipping generated or unsupported file: {f.name} ---")
                    continue
                files_to_process.append(f)
            
            total_count = len(files_to_process)
            
            if total_count == 0:
                print("⚠️ No valid report files found to process.")
            else:
                # Process each report file
                for i, f in enumerate(files_to_process, start=1):
                    print(f"\n--- Analyzing Report {i}/{total_count}: {f.name} ---")
                    
                    # Parse the report using LLM-based extraction
                    result = parse_report(str(f))

                    # Display extracted information
                    print(f"  File: {result['file']}")
                    print(f"  Name: {result['employee_name']}")
                    print(f"  ID: {result['employee_id']}")
                    print(f"  Period: {result['report_period']}")
                    summary_str = json.dumps(result.get('report_summary'), indent=4, ensure_ascii=False)
                    print(f"  Summary:\n{summary_str}")
                    
                    # Delete processed file to save disk space
                    try:
                        os.remove(f)
                        if os.getenv("DEBUG", "False").lower() == "true":
                            print(f"  🗑️ Removed local file: {f.name}")
                    except OSError as e:
                        print(f"  ⚠️ Could not remove file {f.name}: {e}")

    print("\n✅ Process complete.")

    # Step 3: Export validated data to Excel with summary table
    export_summary_excel(ALL_RESULTS, EMPLOYEE_NAMES)
    
    # Step 4: Update Google Sheets with validated and matched data
    update_google_sheets(ALL_RESULTS, EMPLOYEE_NAMES)