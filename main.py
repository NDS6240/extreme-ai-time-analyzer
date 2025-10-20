import json
from gmail_fetcher import fetch_reports_from_gmail
from report_parser import parse_report  # Import the new LLM-based parser
from pathlib import Path

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
                # Only attempt to parse supported file types
                if f.is_file() and f.suffix.lower() in supported_extensions:
                    print(f"\n--- Analyzing: {f.name} ---")
                    result = parse_report(str(f))

                    # Print the detailed results
                    print(f"  File: {result['file']}")
                    print(f"  Name: {result['employee_name']}")
                    print(f"  ID: {result['employee_id']}")
                    print(f"  Period: {result['report_period']}")
                    # Pretty print the summary totals (using the new key 'report_summary')
                    summary_str = json.dumps(result.get('report_summary'), indent=4, ensure_ascii=False)
                    print(f"  Summary:\n{summary_str}")

                elif f.is_file():
                    # Acknowledge unsupported files
                    print(f"--- Skipping unsupported file: {f.name} ---")

    print("\n✅ Process complete.")
