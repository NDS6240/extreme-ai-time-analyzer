import pandas as pd
from datetime import datetime
from pathlib import Path


def export_summary_excel(all_results):
    if not all_results:
        print("⚠️ No results to export to Excel.")
        return

    flattened = []
    for item in all_results:
        base = {k: v for k, v in item.items() if k != "report_summary"}
        summary = item.get("report_summary", {}) or {}
        base.update(summary)
        flattened.append(base)

    df = pd.DataFrame(flattened)
    date_str = datetime.now().strftime("%Y-%m-%d")
    reports_folder = Path.cwd() / "downloads" / "reports"
    reports_folder.mkdir(parents=True, exist_ok=True)
    out_path = reports_folder / f"all_reports_summary_{date_str}.xlsx"
    df.to_excel(out_path, index=False)
    print(f"✅ Excel summary exported: {out_path}")
