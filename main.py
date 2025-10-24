import json
from gmail_fetcher import fetch_reports_from_gmail
from report_parser import parse_report  # Import the new LLM-based parser
from pathlib import Path
from report_parser import ALL_RESULTS
from export_to_excel import export_summary_excel

if __name__ == "__main__":
    print("📩 Fetching new reports from Gmail...")
    fetch_reports_from_gmail()

    print("\n🧠 Analyzing downloaded reports...\n")
    downloads_path = Path("downloads")
    if not downloads_path.exists():
        print("⚠️ No 'downloads' folder found. Please ensure files are downloaded correctly.")
    else:
        files = list(downloads_path.glob("*"))
        if not files:
            print("⚠️ No files found in 'downloads' folder.")
        else:
            supported_extensions = [".pdf", ".xlsx", ".xls", ".csv"]

            for f in files:
                # Skip already processed or generated files
                if "_parsed" in f.name or f.name.startswith(".") or f.suffix.lower() not in supported_extensions:
                    print(f"--- Skipping generated or unsupported file: {f.name} ---")
                    continue

                # Parse only original report files
                print(f"\n--- Analyzing: {f.name} ---")
                result = parse_report(str(f))

                # Print detailed results
                print(f"  File: {result['file']}")
                print(f"  Name: {result['employee_name']}")
                print(f"  ID: {result['employee_id']}")
                period = result.get('report_month') or result.get('report_period')
                print(f"  Period: {period}")
                summary_str = json.dumps(result.get('report_summary'), indent=4, ensure_ascii=False)
                print(f"  Summary:\n{summary_str}")

    print("\n✅ Process complete.")

    export_summary_excel(ALL_RESULTS)
