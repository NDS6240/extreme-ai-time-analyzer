import gspread
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
import re

# --- Constants ---
MASTER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1E7GXeSoh48lCNBzpsHYmiE1TdeZCWYKaIOVOAb3TWa4"
DATA_SHEET_URL = "https://docs.google.com/spreadsheets/d/1TtbdGfBNbAIqU7_W-F90gPcA1T_OyxA2GyvBPv6uL4c"
CREDENTIALS_FILE = "credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]

# --- NEW: Define target column name ---
TARGET_COLUMN_TITLE = "שעות בפועל"


# --- Authentication ---
def authenticate():
    """Connects to Google Sheets using the credentials file."""
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except FileNotFoundError:
        print(f"❌ ERROR: Credentials file not found: '{CREDENTIALS_FILE}'.")
        print("     Please ensure the .json file is in the root folder and named correctly.")
        return None
    except Exception as e:
        print(f"❌ Google authentication error: {repr(e)}")
        return None


# --- Deduplication Logic (Unchanged from original) ---
def deduplicate_results(all_results):
    """
    Cleans the results list using smart deduplication before uploading.
    Keeps the record with the most data.
    """
    if not all_results:
        return []

    flattened = []
    for item in all_results:
        base = {k: v for k, v in item.items() if k != "report_summary"}
        summary = item.get("report_summary", {}) or {}
        base.update(summary)
        flattened.append(base)

    df = pd.DataFrame(flattened)
    if df.empty:
        return []

    # 1. Normalize keys
    df['norm_name'] = df['employee_name'].astype(str).str.strip().str.replace(r'[-"\']', ' ', regex=True).str.replace(
        r'\s+', ' ', regex=True).replace('None', 'UNKNOWN').replace('', 'UNKNOWN')
    df['norm_id'] = df['employee_id'].fillna('NO_ID').replace('', 'NO_ID').astype(str)
    df['norm_id'] = df['norm_id'].replace('None', 'NO_ID')

    # 2. Create "quality score"
    df['has_id'] = (df['norm_id'] != 'NO_ID').astype(int)
    hour_cols = [
        'total_presence_hours', 'total_approved_hours', 'total_payable_hours',
        'overtime_hours', 'vacation_days', 'sick_days', 'holiday_days'
    ]
    for col in hour_cols:
        if col not in df.columns:
            df[col] = np.nan
    df['hour_count'] = df[hour_cols].notna().sum(axis=1)

    # 3. Sort by quality
    df = df.sort_values(
        by=['norm_name', 'has_id', 'hour_count'],
        ascending=[True, False, False]
    )

    # 4. Smart deduplication
    deduped_df = df.drop_duplicates(subset=['norm_name', 'norm_id'], keep='first')

    # 5. Clean up helper columns and return list of dicts
    final_df = deduped_df.drop(columns=['norm_name', 'norm_id', 'has_id', 'hour_count'], errors='ignore')

    final_results = []
    for _, row in final_df.iterrows():
        result_item = {
            "file": row.get('file'),
            "employee_name": row.get('employee_name'),
            "employee_id": row.get('employee_id'),
            "report_period": row.get('report_period'),
            "report_summary": {}
        }

        summary_keys = set(row.index) - {'file', 'employee_name', 'employee_id', 'report_period'}
        for key in summary_keys:
            if pd.notna(row[key]):
                result_item["report_summary"][key] = row[key]

        final_results.append(result_item)

    return final_results


# --- Main Update Function (MODIFIED) ---
def update_google_sheets(all_results):
    """
    Main function: uploads processed results to Google Sheets.
    MODIFIED: Only updates the 'שעות בפועל' column, but still creates new tabs from master.
    """
    print("\n🔄 Starting Google Sheets update...")

    client = authenticate()
    if not client:
        return

    # 1. Deduplicate results (remains the same, gets full data)
    print("   - Performing deduplication...")
    clean_results = deduplicate_results(all_results)
    if not clean_results:
        print("⚠️ No data to update in Google Sheets (list is empty after cleaning).")
        return

    # Determine report period (remains the same)
    report_period = None
    for res in clean_results:
        report_period = res.get('report_period')
        if report_period:
            break

    if not report_period:
        print("⚠️ Cannot determine report period from results. Stopping.")
        return
    report_period = re.sub(r'[\"\',]', '', str(report_period)).strip()
    if not report_period:
        print("⚠️ Report period name is invalid after cleaning. Stopping.")
        return

    print(f"   - Report period identified: {report_period}")

    try:
        # 2. Open Sheets
        print("   - Opening 'master' and 'data' sheets...")
        master_sheet = client.open_by_url(MASTER_SHEET_URL)
        data_sheet = client.open_by_url(DATA_SHEET_URL)

        # 3. Read employee list from Master (remains the same)
        try:
            master_ws = master_sheet.get_worksheet(0)  # First tab
            master_data = master_ws.get_all_values()
            if not master_data:
                print("⚠️ Master sheet is empty. Cannot create template.")
                return
            master_headers = master_data[0] # Now includes 'תקן שעות'
            master_employees_rows = master_data[1:]
            master_employee_names = [row[0] for row in master_employees_rows if row]
            print(f"   - {len(master_employee_names)} employees loaded from master.")
        except Exception as e:
            print(f"❌ Error reading master sheet: {repr(e)}")
            print("   - Ensure the master sheet is set up with columns in the first tab.")
            return

        # 4. Check/Create monthly tab (MODIFIED)
        try:
            monthly_ws = data_sheet.worksheet(report_period)
            print(f"   - Sheet '{report_period}' exists. Updating data...")
        except WorksheetNotFound:
            print(f"   - Sheet '{report_period}' not found. Creating new sheet...")
            monthly_ws = data_sheet.add_worksheet(title=report_period, rows=100, cols=30)

            # --- Create headers in the new sheet (MODIFIED) ---
            # 'תקן שעות' is now part of master_headers
            data_headers = master_headers + [
                # 'תקן שעות', <-- REMOVED! It will be copied from master_headers
                TARGET_COLUMN_TITLE, # 'שעות בפועל'
                'file', # For debugging
                'employee_id' # For debugging
            ]
            
            monthly_ws.append_row(data_headers)

            # Copy employee list from master
            if master_employees_rows:
                monthly_ws.append_rows(master_employees_rows)
            print(f"   - Sheet '{report_period}' created with {len(master_employees_rows)} employees.")

        # 5. Map data for update (MODIFIED)
        print("   - Mapping updated data...")

        all_data = monthly_ws.get_all_values()
        if not all_data:
            print("❌ Monthly sheet is empty and was not created correctly. Stopping.")
            return

        headers = all_data[0]
        col_map = {header.strip(): i + 1 for i, header in enumerate(headers)}  # +1 for gspread
        master_name_col_header = master_headers[0] # e.g., "שם עובד"
        
        # --- Critical check for required columns ---
        if master_name_col_header not in col_map:
            print(f"❌ Cannot find name column '{master_name_col_header}' in data sheet. Check setup.")
            return
        if TARGET_COLUMN_TITLE not in col_map:
            print(f"❌ Cannot find target column '{TARGET_COLUMN_TITLE}' in data sheet. Check setup.")
            return

        # Build row map (employee_name -> row_number) (remains the same)
        row_map = {}
        for i, row in enumerate(all_data[1:], start=2):  # +1 for header skip, start=2 for gspread
            if len(row) > (col_map[master_name_col_header] - 1):
                employee_name = row[col_map[master_name_col_header] - 1].strip()
                if employee_name:
                    name_norm = re.sub(r'[-"\']', ' ', employee_name)
                    name_norm = re.sub(r'\s+', ' ', name_norm).strip()
                    if name_norm not in row_map:
                        row_map[name_norm] = i

        updates_to_send = []

        for result in clean_results:
            name = result.get('employee_name')
            if not name:
                continue

            # Normalize name for matching (remains the same)
            name_norm = re.sub(r'[-"\']', ' ', name.strip())
            name_norm = re.sub(r'\s+', ' ', name_norm).strip()

            row_num = row_map.get(name_norm)
            if not row_num:
                row_num = row_map.get(name.strip())
                if not row_num:
                    # Logic for adding new employee if not found (remains the same)
                    if name_norm in [re.sub(r'\s+', ' ', re.sub(r'[-"\']', ' ', n)).strip() for n in
                                     master_employee_names]:
                        print(
                            f"   - ⚠️ Warning: Employee '{name}' ({name_norm}) is in master but not found in sheet row map. Check for duplicates in sheet.")
                        continue
                    else:
                        print(f"   - ℹ️ New employee '{name}' not found in master, adding to sheet.")
                        new_row_data = ["" for _ in headers]
                        new_row_data[col_map[master_name_col_header] - 1] = name.strip()
                        monthly_ws.append_row(new_row_data)
                        row_num = len(all_data) + 1
                        all_data.append(new_row_data)
                        row_map[name_norm] = row_num

            # --- SIMPLIFIED UPDATE LOGIC (THE CORE CHANGE) ---
            if row_num:
                
                # 1. Update 'file' column (if it exists)
                file_val = result.get('file')
                if 'file' in col_map and file_val is not None:
                    col = col_map['file']
                    updates_to_send.append(gspread.Cell(row_num, col, str(file_val)))

                # 2. Update 'employee_id' column (if it exists)
                id_val = result.get('employee_id')
                if 'employee_id' in col_map and id_val is not None:
                    col = col_map['employee_id']
                    updates_to_send.append(gspread.Cell(row_num, col, str(id_val)))

                # 3. Update 'שעות בפועל' (TARGET_COLUMN_TITLE) column
                # We get the full summary, but only use one value from it
                summary = result.get('report_summary', {})
                hours_val = summary.get('total_presence_hours') # <-- The specific value
                
                if hours_val is not None:
                    col = col_map[TARGET_COLUMN_TITLE]
                    updates_to_send.append(gspread.Cell(row_num, col, str(hours_val)))
                
                # --- REMOVED: Loop for all other summary.items() ---

        # Send all updates in one batch
        if updates_to_send:
            monthly_ws.update_cells(updates_to_send, value_input_option='USER_ENTERED')
            print(f"✅ Google Sheets update complete! {len(updates_to_send)} cells updated.")
        else:
            print("✅ Google Sheets is already up to date. No changes.")

    except Exception as e:
        print(f"❌ General error during Google Sheets update: {repr(e)}")