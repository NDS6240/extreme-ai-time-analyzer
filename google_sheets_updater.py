"""
Google Sheets Updater Module
Updates Google Sheets with validated employee attendance data.
Uses master_employee.json and Google Sheets Master for name matching.
"""
import gspread
import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
import re
import difflib
from data_validator import (
    load_master_data, 
    get_master_employee_dict, 
    match_employee_name, 
    normalize_name
)

# Google Sheets URLs and configuration
MASTER_SHEET_URL = "https://docs.google.com/spreadsheets/d/1E7GXeSoh48lCNBzpsHYmiE1TdeZCWYKaIOVOAb3TWa4"
DATA_SHEET_URL = "https://docs.google.com/spreadsheets/d/1TtbdGfBNbAIqU7_W-F90gPcA1T_OyxA2GyvBPv6uL4c"
CREDENTIALS_FILE = "credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]

# Column titles in Hebrew
TARGET_COLUMN_TITLE = "שעות בפועל"  # Actual hours column
CALCULATED_COLUMN_TITLE = "שעות שבוצעו מתוך תקן (%)"  # Calculated percentage column


def _fix_rtl_name(name: str) -> str:
    """
    Simple RTL name fix - reverses name if it contains Hebrew characters.
    
    Args:
        name: Employee name string
        
    Returns:
        Reversed name if Hebrew detected, original name otherwise
    """
    if not name or not isinstance(name, str):
        return name
    # If name contains Hebrew characters, reverse it
    return name[::-1] if any('\u0590' <= c <= '\u05FF' for c in name) else name


def get_best_name_match(employee_name: str, master_names: list) -> str:
    """
    Find best matching employee name from master list using fuzzy matching.
    Checks both original and reversed (RTL) name.
    
    Args:
        employee_name: Name to match
        master_names: List of master employee names
        
    Returns:
        Best matching name from master list, or "**CHECK: {name}" if no match found
    """
    if not employee_name or not master_names:
        if not master_names:
            return f"**CHECK: {employee_name}" if employee_name else employee_name
        return employee_name
    
    # Normalize employee name
    name_norm = str(employee_name).strip()
    name_norm = name_norm.replace('-', ' ').replace('"', ' ').replace("'", ' ')
    name_norm = ' '.join(name_norm.split())
    
    # First check: original name
    best_match = None
    best_ratio = 0.0
    
    for master_name in master_names:
        master_norm = str(master_name).strip()
        master_norm = master_norm.replace('-', ' ').replace('"', ' ').replace("'", ' ')
        master_norm = ' '.join(master_norm.split())
        
        # Calculate similarity ratio using difflib
        ratio = difflib.SequenceMatcher(None, name_norm.lower(), master_norm.lower()).ratio()
        if ratio > best_ratio and ratio >= 0.8:
            best_ratio = ratio
            best_match = master_name
    
    # Second check: reversed name (RTL fix)
    if not best_match:
        name_reversed = _fix_rtl_name(name_norm)
        if name_reversed != name_norm:  # Only if name changed after reversal
            for master_name in master_names:
                master_norm = str(master_name).strip()
                master_norm = master_norm.replace('-', ' ').replace('"', ' ').replace("'", ' ')
                master_norm = ' '.join(master_norm.split())
                
                ratio = difflib.SequenceMatcher(None, name_reversed.lower(), master_norm.lower()).ratio()
                if ratio > best_ratio and ratio >= 0.8:
                    best_ratio = ratio
                    best_match = master_name
    
    # Return matched name from master list if found
    if best_match:
        return best_match
    
    # Add CHECK prefix if no match found
    return f"**CHECK: {employee_name}"


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


def deduplicate_results(all_results, employee_names=None):
    """
    Cleans the results list using smart deduplication before uploading.
    Keeps the record with the most data.
    Supports name unification with master list.
    
    Args:
        all_results: List of parsed report results
        employee_names: Optional list of master employee names for unification
        
    Returns:
        List of deduplicated results
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

    # Step 1: Normalize keys with name unification (bidirectional RTL fix)
    if employee_names:
        df['unified_name'] = df['employee_name'].apply(
            lambda x: get_best_name_match(x, employee_names) if pd.notna(x) else x
        )
        # Use unified name for normalization
        df['norm_name'] = df['unified_name'].astype(str).str.strip().str.replace(r'[-"\']', ' ', regex=True).str.replace(
            r'\s+', ' ', regex=True).replace('None', 'UNKNOWN').replace('', 'UNKNOWN')
        # Update employee_name to unified name
        df['employee_name'] = df['unified_name']
        df = df.drop(columns=['unified_name'], errors='ignore')
    else:
        # Without master list - regular normalization
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

def col_to_letter(col_index):
    """
    Convert column index to A1 notation (e.g., 3 -> C, 27 -> AA).
    
    Args:
        col_index: 1-based column index
        
    Returns:
        Column letter(s) in A1 notation
    """
    letter = ''
    while col_index > 0:
        col_index, remainder = divmod(col_index - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter


def update_google_sheets(all_results, employee_names=None):
    """
    Main function: uploads processed results to Google Sheets.
    Uses enhanced name matching with master_employee.json and Google Sheets Master.
    Adds calculated column for percentage of standard hours.
    
    Args:
        all_results: List of parsed report results
        employee_names: Optional list of master employee names
    """
    print("\n🔄 Starting Google Sheets update...")

    client = authenticate()
    if not client:
        return

    # 1. Deduplicate results
    print("   - Performing deduplication...")
    clean_results = deduplicate_results(all_results, employee_names)
    
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

        # Step 3: Read employee list from Master Sheet
        try:
            master_ws = master_sheet.get_worksheet(0)  # First tab
            master_data = master_ws.get_all_values()
            if not master_data:
                print("⚠️ Master sheet is empty. Cannot create template.")
                return
            # Column [0] = Employee Name, [2] = Standard Hours (assuming 3 columns)
            master_headers = master_data[0]
            master_employees_rows = master_data[1:]
            master_employee_names = [row[0] for row in master_employees_rows if row]
            print(f"   - {len(master_employee_names)} employees loaded from master.")
        except Exception as e:
            print(f"❌ Error reading master sheet: {repr(e)}")
            print("   - Ensure the master sheet is set up with columns in the first tab.")
            return

        # Step 4: Check/Create monthly tab for the report period
        try:
            monthly_ws = data_sheet.worksheet(report_period)
            print(f"   - Sheet '{report_period}' exists. Updating data...")
        except WorksheetNotFound:
            print(f"   - Sheet '{report_period}' not found. Creating new sheet...")
            monthly_ws = data_sheet.add_worksheet(title=report_period, rows=100, cols=30)

            # Create headers in the new sheet
            data_headers = master_headers + [
                TARGET_COLUMN_TITLE,  # Actual hours column
                CALCULATED_COLUMN_TITLE,  # Calculated percentage column
                'file',  # For debugging
                'employee_id'  # For debugging
            ]
            
            monthly_ws.append_row(data_headers)

            # Copy employee list from master
            if master_employees_rows:
                monthly_ws.append_rows(master_employees_rows)
            print(f"   - Sheet '{report_period}' created with {len(master_employees_rows)} employees.")
            
            # Add percentage formula to newly created sheet
            current_headers = monthly_ws.get_all_values()[0]
            current_col_map = {header.strip(): i + 1 for i, header in enumerate(current_headers)}
            
            # Assuming 'תקן שעות' (Standard Hours) is the 3rd column (index 2) in the master sheet
            standard_hours_col_index = 3 
            standard_hours_header = master_headers[standard_hours_col_index - 1].strip()
            
            actual_hours_col = current_col_map.get(TARGET_COLUMN_TITLE)
            standard_hours_col = current_col_map.get(standard_hours_header)
            calculated_col = current_col_map.get(CALCULATED_COLUMN_TITLE)
            
            if actual_hours_col and standard_hours_col and calculated_col:
                # Convert column index to A1 notation
                actual_letter = col_to_letter(actual_hours_col)
                standard_letter = col_to_letter(standard_hours_col)
                
                num_employees = len(master_employees_rows)
                
                if num_employees > 0:
                    formulas_to_send = []
                    
                    # Start from row 2 (after header)
                    for row_index in range(2, num_employees + 2):
                        # Formula: IFERROR(Actual Hours / Standard Hours, "")
                        formula = f'=IFERROR({actual_letter}{row_index}/{standard_letter}{row_index}, "")'
                        formulas_to_send.append(gspread.Cell(row_index, calculated_col, formula))

                    monthly_ws.update_cells(formulas_to_send, value_input_option='USER_ENTERED')
                    print(f"   - Added percentage formula to {num_employees} rows.")
                    
        # Step 5: Map data for update with enhanced matching
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

        # Load master employee data for better matching
        load_master_data()
        master_dict = get_master_employee_dict()
        master_names_from_json = list(master_dict.keys()) if master_dict else []
        
        # Build enhanced row map (employee_name -> row_number) with multiple variants
        # Maps both normalized names and original names for flexible matching
        row_map = {}
        row_map_variants = {}  # For fuzzy matching: normalized_name_lower -> (original_name, row_number)
        
        for i, row in enumerate(all_data[1:], start=2):  # +1 for header skip, start=2 for gspread
            if len(row) > (col_map[master_name_col_header] - 1):
                employee_name = row[col_map[master_name_col_header] - 1].strip()
                if employee_name:
                    # Store exact name
                    if employee_name not in row_map:
                        row_map[employee_name] = i
                    
                    # Store normalized variants
                    name_norm = normalize_name(employee_name)
                    if name_norm:
                        name_norm_lower = name_norm.lower()
                        if name_norm_lower not in row_map_variants:
                            row_map_variants[name_norm_lower] = (employee_name, i)
                        # Also store with different normalizations
                        name_alt = re.sub(r'[-"\']', ' ', employee_name).strip().lower()
                        if name_alt and name_alt != name_norm_lower:
                            if name_alt not in row_map_variants:
                                row_map_variants[name_alt] = (employee_name, i)

        print(f"   - Built row map with {len(row_map)} exact matches and {len(row_map_variants)} normalized variants")
        
        updates_to_send = []
        unmatched_count = 0
        matched_count = 0

        for result in clean_results:
            name = result.get('employee_name')
            if not name:
                continue
            
            # Remove CHECK prefix if exists for matching
            original_name = name
            if name.startswith('**CHECK:'):
                name = name.replace('**CHECK:', '').strip()

            row_num = None
            matched_master_name = None
            
            # Strategy 1: Try exact match first
            name_norm = normalize_name(name)
            if name_norm:
                name_norm_lower = name_norm.lower()
                if name_norm_lower in row_map_variants:
                    matched_name_in_sheet, row_num = row_map_variants[name_norm_lower]
                    matched_master_name = matched_name_in_sheet
                    matched_count += 1
                    print(f"   ✅ Exact match: '{name}' -> '{matched_name_in_sheet}' (row {row_num})")
            
            # Strategy 2: Try matching with master_employee.json (best source)
            if not row_num and master_names_from_json:
                matched_name, match_ratio = match_employee_name(name, master_names_from_json, threshold=0.75)
                if matched_name and match_ratio >= 0.75:
                    matched_master_name = matched_name
                    # Now try to find this matched name in the sheet
                    matched_norm = normalize_name(matched_name).lower()
                    if matched_norm in row_map_variants:
                        _, row_num = row_map_variants[matched_norm]
                        matched_count += 1
                        print(f"   ✅ Master JSON match: '{name}' -> '{matched_name}' (row {row_num}, ratio: {match_ratio:.2%})")
            
            # Strategy 3: Try matching with master_employee_names from Google Sheet
            if not row_num and master_employee_names:
                matched_name, match_ratio = match_employee_name(name, master_employee_names, threshold=0.75)
                if matched_name and match_ratio >= 0.75:
                    matched_master_name = matched_name
                    # Try to find in sheet
                    matched_norm = normalize_name(matched_name).lower()
                    if matched_norm in row_map_variants:
                        _, row_num = row_map_variants[matched_norm]
                        matched_count += 1
                        print(f"   ✅ Master Sheet match: '{name}' -> '{matched_name}' (row {row_num}, ratio: {match_ratio:.2%})")
            
            # Strategy 4: Fuzzy match against all names in row_map_variants
            if not row_num:
                best_match_in_sheet = None
                best_ratio = 0.0
                name_norm_lower = normalize_name(name).lower()
                
                for sheet_name_norm_lower, (sheet_name_original, sheet_row_num) in row_map_variants.items():
                    ratio = difflib.SequenceMatcher(None, name_norm_lower, sheet_name_norm_lower).ratio()
                    if ratio > best_ratio and ratio >= 0.75:
                        best_ratio = ratio
                        best_match_in_sheet = sheet_name_original
                        row_num = sheet_row_num
                
                if row_num:
                    matched_master_name = best_match_in_sheet
                    matched_count += 1
                    print(f"   ✅ Fuzzy match: '{name}' -> '{best_match_in_sheet}' (row {row_num}, ratio: {best_ratio:.2%})")
            
            # If still no match, add new row or skip
            if not row_num:
                unmatched_count += 1
                # Check if name exists in master list (to avoid false positives)
                is_in_master = False
                if master_names_from_json:
                    matched_name, match_ratio = match_employee_name(name, master_names_from_json, threshold=0.5)
                    if matched_name and match_ratio >= 0.5:
                        is_in_master = True
                
                name_norm_for_check = normalize_name(name).lower()
                is_in_sheet_master = name_norm_for_check in [normalize_name(n).lower() for n in master_employee_names]
                
                if is_in_master or is_in_sheet_master:
                    print(f"   - ⚠️ Warning: Employee '{name}' exists in master but not found in sheet. Skipping.")
                else:
                    print(f"   - ℹ️ New employee '{name}' not found anywhere, adding to sheet.")
                    new_row_data = ["" for _ in headers]
                    new_row_data[col_map[master_name_col_header] - 1] = name.strip()
                    monthly_ws.append_row(new_row_data)
                    row_num = len(all_data) + 1
                    all_data.append(new_row_data)
                    # Update maps
                    row_map[name] = row_num
                    if name_norm_for_check:
                        row_map_variants[name_norm_for_check] = (name, row_num)

            # Update actual hours column
            if row_num:
                summary = result.get('report_summary', {})
                hours_val = summary.get('total_presence_hours')
                
                if hours_val is not None:
                    col = col_map[TARGET_COLUMN_TITLE]
                    updates_to_send.append(gspread.Cell(row_num, col, str(hours_val)))
                
        # Send all updates in one batch
        if updates_to_send:
            monthly_ws.update_cells(updates_to_send, value_input_option='USER_ENTERED')
            print(f"✅ Google Sheets update complete!")
            print(f"   - {matched_count} employees matched and updated")
            print(f"   - {unmatched_count} employees could not be matched")
            print(f"   - {len(updates_to_send)} cells updated total")
        else:
            print("✅ Google Sheets is already up to date. No changes.")

    except Exception as e:
        print(f"❌ General error during Google Sheets update: {repr(e)}")