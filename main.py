from gmail_fetcher import fetch_reports_from_gmail
from report_parser import parse_report
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
            for f in files:
                result = parse_report(f)
                print(f"📄 File: {result['file']} | Type: {result['type']} | Total Hours: {result['total_hours']}")

    print("\n✅ Process complete.")
