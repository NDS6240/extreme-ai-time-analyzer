import pandas as pd
from datetime import datetime
from pathlib import Path
import numpy as np


def export_summary_excel(all_results):
    if not all_results:
        print("⚠️ No results to export to Excel.")
        return

    flattened = []
    for item in all_results:
        base = {k: v for k, v in item.items() if k != "report_summary"}
        summary = item.get("report_summary", {}) or {}

        # מבטיח שגם אם הדוח ריק, המפתחות קיימים כ-None
        base.update(summary)
        flattened.append(base)

    if not flattened:
        print("⚠️ No results to export to Excel (list was empty).")
        return

    df = pd.DataFrame(flattened)
    original_count = len(df)

    if original_count == 0:
        print("⚠️ No data in DataFrame to export.")
        return

    # --- START: Deduplication Logic ---

    # 1. נירמול שדות מפתח להשוואה חכמה
    # --- התיקון נמצא כאן ---
    # מחליף מקפים/גרשים ברווח, ואז מנקה רווחים כפולים
    df['norm_name'] = df['employee_name'].astype(str).str.strip().str.replace(r'[-"\']', ' ', regex=True).str.replace(
        r'\s+', ' ', regex=True).replace('None', 'UNKNOWN').replace('', 'UNKNOWN')
    # --- סוף התיקון ---

    # ניקוי מספרי עובד (הפיכת None, '' וערכים לא מספריים ל-NO_ID)
    df['norm_id'] = df['employee_id'].fillna('NO_ID').replace('', 'NO_ID').astype(str)
    df['norm_id'] = df['norm_id'].replace('None', 'NO_ID')

    # 2. יצירת "ציון איכות" (Completeness Score)
    df['has_id'] = (df['norm_id'] != 'NO_ID').astype(int)

    hour_cols = ['total_presence_hours', 'total_approved_hours', 'total_payable_hours', 'overtime_hours',
                 'vacation_days', 'sick_days']
    # ודא שכל העמודות קיימות לפני הספירה
    for col in hour_cols:
        if col not in df.columns:
            df[col] = np.nan

    df['hour_count'] = df[hour_cols].notna().sum(axis=1)

    # 3. מיון לפי איכות: מהשורה הטובה ביותר לגרועה ביותר
    df = df.sort_values(
        by=['norm_name', 'has_id', 'hour_count'],
        ascending=[True, False, False]
    )

    # 4. הסרת כפילויות חכמה
    deduped_df = df.drop_duplicates(subset=['norm_name', 'norm_id'], keep='first')

    # 5. ניקוי עמודות העזר לפני הייצוא
    final_df = deduped_df.drop(columns=['norm_name', 'norm_id', 'has_id', 'hour_count'], errors='ignore')

    # --- END: Deduplication Logic ---

    final_count = len(final_df)

    date_str = datetime.now().strftime("%Y-%m-%d")
    reports_folder = Path.cwd() / "downloads" / "reports"
    reports_folder.mkdir(parents=True, exist_ok=True)
    out_path = reports_folder / f"all_reports_summary_{date_str}.xlsx"

    # שימוש ב-DataFrame המסונן
    final_df.to_excel(out_path, index=False, engine='openpyxl')

    print(f"✅ Excel summary exported: {out_path}")
    if (original_count > final_count):
        print(
            f"ℹ️ Deduplication removed {original_count - final_count} duplicate records (Original: {original_count}, Final: {final_count}).")
