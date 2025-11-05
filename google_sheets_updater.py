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
from datetime import datetime, timedelta

# --- שונה: ייבוא הרשימה המרכזית ---
try:
    from report_parser import EMPLOYEE_NAMES as MASTER_EMPLOYEE_LIST
except ImportError:
    print("CRITICAL: Could not import EMPLOYEE_NAMES from report_parser.")
    MASTER_EMPLOYEE_LIST = []
# --- סוף קטע חדש ---

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
    # --- חדש: בדיקה למניעת כפילות ---
    if employee_name and employee_name.startswith("**CHECK:"):
        return employee_name # השם כבר סומן כבעייתי, החזר אותו כמו שהוא
    # --- סוף קטע חדש ---
    
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
        if ratio > best_ratio and ratio >= 0.7:
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
                if ratio > best_ratio and ratio >= 0.7:
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

    # --- שונה: שימוש ברשימה המרכזית ---
    if employee_names is None:
        employee_names = MASTER_EMPLOYEE_LIST
    # --- סוף שינוי ---

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
        # --- חדש: תיקון כפילות CHECK ---
        df['unified_name'] = df['employee_name'].apply(
            lambda x: x if x and x.startswith("**CHECK:") else get_best_name_match(x, employee_names) if pd.notna(x) else x
        )
        # --- סוף קטע חדש ---
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


def _parse_period_to_date(period_name: str):
    """
    Parse a period name like "October 2025" or "יוני 2023" into a datetime(year, month, 1).
    Returns None if parsing fails.
    """
    if not period_name:
        return None
    try:
        text = str(period_name).strip()
        text = re.sub(r'["\']', '', text)

        eng_months = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
            'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        heb_months = {
            'ינואר': 1, 'פברואר': 2, 'מרץ': 3, 'אפריל': 4, 'מאי': 5, 'יוני': 6,
            'יולי': 7, 'אוגוסט': 8, 'ספטמבר': 9, 'אוקטובר': 10, 'נובמבר': 11, 'דצמבר': 12
        }

        # Try English "Month YYYY"
        m = re.match(r'^([A-Za-z]+)\s+(\d{4})$', text)
        if m:
            month_name = m.group(1).lower()
            year = int(m.group(2))
            month = eng_months.get(month_name)
            if month:
                return datetime(year, month, 1)

        # Try Hebrew "<month> <year>"
        m2 = re.match(r'^([\u0590-\u05FF"\'-]+)\s+(\d{4})$', text)
        if m2:
            month_name_he = m2.group(1).replace('"', '').replace("'", '').strip()
            year = int(m2.group(2))
            month = heb_months.get(month_name_he)
            if month:
                return datetime(year, month, 1)

        # Fallback: try only year-month numeric like 2025-10 or 10/2025
        m3 = re.match(r'^(\d{4})[-/](\d{1,2})$', text)
        if m3:
            year = int(m3.group(1))
            month = int(m3.group(2))
            if 1 <= month <= 12:
                return datetime(year, month, 1)

    except Exception:
        return None
    return None


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
    
    # --- שונה: שימוש ברשימה המרכזית ---
    if employee_names is None:
        employee_names = MASTER_EMPLOYEE_LIST
    # --- סוף שינוי ---

    client = authenticate()
    if not client:
        return

    # --- Group results by period name first ---
    results_by_period = {}
    for res in (all_results or []):
        period = res.get('report_period')
        if not period:
            continue
        period = re.sub(r'[\"\',]', '', str(period)).strip()
        if not period:
            continue
        results_by_period.setdefault(period, []).append(res)

    if not results_by_period:
        print("⚠️ No report periods found in results. Nothing to update.")
        return

    # Current date for locking and correction logic
    current_date = datetime.now()
    CORRECTION_WINDOW_DAYS = 7 
    
    # --- תיקון שם התקופה עבור הגשות מוקדמות ---
    corrected_results_by_period = {}
    for period, results in results_by_period.items():
        period_clean = re.sub(r'[\"\',]', '', str(period)).strip()
        period_date = _parse_period_to_date(period_clean)
        
        final_period_name = period_clean
        
        if (
            period_date is not None
            and current_date.day <= CORRECTION_WINDOW_DAYS
            and period_date.year == current_date.year
            and period_date.month == current_date.month
        ):
            # אם זה מוקדם בחודש (עד ה-7) והדוח הוא לחודש הנוכחי (למשל נובמבר)
            last_month_date = current_date - timedelta(days=15)
            last_month_name_heb = {
                1: "ינואר", 2: "פברואר", 3: "מרץ", 4: "אפריל", 5: "מאי", 6: "יוני",
                7: "יולי", 8: "אוגוסט", 9: "ספטמבר", 10: "אוקטובר", 11: "נובמבר", 12: "דצמבר"
            }[last_month_date.month]
            
            final_period_name = f"{last_month_name_heb} {last_month_date.year}"
            print(f"   - ⚠️ WARNING: Early '{period_clean}' submission. Correcting period to '{final_period_name}'.")

        # קבץ את התוצאות לפי השם *המתוקן*
        corrected_results_by_period.setdefault(final_period_name, []).extend(results)

    # השתמש במילון המתוקן להמשך העדכון
    results_by_period = corrected_results_by_period
    # --- סוף קטע חדש ---


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
            
            master_headers = master_data[0]
            master_employees_rows = master_data[1:]
            
            # --- שונה: שימוש ברשימה המרכזית במקום ברשימה מהשיט ---
            master_employee_names = MASTER_EMPLOYEE_LIST
            print(f"   - {len(master_employee_names)} employees loaded from (code) master list.")
            # --- סוף שינוי ---
            
        except Exception as e:
            print(f"❌ Error reading master sheet: {repr(e)}")
            print("   - Ensure the master sheet is set up with columns in the first tab.")
            return

        # Iterate over each period and process independently with locking
        for report_period, period_results in results_by_period.items():
            # Locking: skip periods older than 2 months
            period_clean = report_period # שימוש בשם המתוקן/נקי
            period_date = _parse_period_to_date(period_clean)
            if period_date is not None:
                month_diff = (current_date.year - period_date.year) * 12 + (current_date.month - period_date.month)
                if month_diff >= 2:
                    print(f"   - 🔒 Skipping update for locked period: {period_clean}")
                    continue

            print(f"   - Processing active period: {period_clean}")

            # Deduplicate only the current period results
            clean_results = deduplicate_results(period_results, employee_names)
            if not clean_results:
                print(f"   - ⚠️ No data to update for period '{period_clean}' after cleaning.")
                continue

            # Step 4: Check/Create monthly tab for this period
            try:
                monthly_ws = data_sheet.worksheet(period_clean)
                print(f"   - Sheet '{period_clean}' exists. Updating data...")
            except WorksheetNotFound:
                print(f"   - Sheet '{period_clean}' not found. Creating new sheet...")
                monthly_ws = data_sheet.add_worksheet(title=period_clean, rows=100, cols=30)

                # Create headers in the new sheet
                # --- שונה: הסרת עמודות מיותרות ---
                data_headers = master_headers + [
                    TARGET_COLUMN_TITLE,  # Actual hours column
                    CALCULATED_COLUMN_TITLE,  # Calculated percentage column
                    # הוסרו: 'file', 'employee_id'
                ]
                # --- סוף קטע שונה ---
                
                monthly_ws.append_row(data_headers)

                # Copy employee list from master
                if master_employees_rows:
                    monthly_ws.append_rows(master_employees_rows)
                print(f"   - Sheet '{period_clean}' created with {len(master_employees_rows)} employees.")
                
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
                    
            # Step 5: Map data for update with enhanced matching for this period
            print("   - Mapping updated data...")

            all_data = monthly_ws.get_all_values()
            if not all_data:
                print(f"❌ Monthly sheet '{period_clean}' is empty and was not created correctly. Skipping.")
                continue
            
            headers = all_data[0]
            col_map = {header.strip(): i + 1 for i, header in enumerate(headers)}  # +1 for gspread
            master_name_col_header = master_headers[0] # e.g., "שם עובד"
            
            # --- Critical check for required columns ---
            if master_name_col_header not in col_map:
                print(f"❌ Cannot find name column '{master_name_col_header}' in data sheet. Check setup.")
                continue
            if TARGET_COLUMN_TITLE not in col_map:
                print(f"❌ Cannot find target column '{TARGET_COLUMN_TITLE}' in data sheet. Check setup.")
                continue
            
            # --- שונה: שימוש ברשימה המרכזית ---
            load_master_data() # עדיין נטען כדי לקבל שעות תקן וחברה
            master_names_from_json = MASTER_EMPLOYEE_LIST
            # --- סוף שינוי ---
            
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
                
                row_num = None
                
                # Strategy 1: Try exact match first
                name_norm = normalize_name(name)
                if name_norm:
                    name_norm_lower = name_norm.lower()
                    if name_norm_lower in row_map_variants:
                        matched_name_in_sheet, row_num = row_map_variants[name_norm_lower]
                        matched_count += 1
                        # print(f"   ✅ Exact match: '{name}' -> '{matched_name_in_sheet}' (row {row_num})") # הפחתת רעש
                
                # --- שונה: אסטרטגיה 2,3,4 (Fuzzy) מאוחדות ---
                # Strategy 2: Fuzzy match against all names in row_map_variants
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
                        matched_count += 1
                        print(f"   ✅ Fuzzy match: '{name}' -> '{best_match_in_sheet}' (row {row_num}, ratio: {best_ratio:.2%})")
                
                
                # --- !!! תיקון מרכזי !!! ---
                # אם לא מצאנו שורה תואמת, הוסף את השורה בסוף הגיליון
                if not row_num:
                    unmatched_count += 1
                    
                    # השם המקורי כבר אמור להכיל "**CHECK:" אם הוא נכשל בולידציה
                    original_name_from_result = result.get('employee_name', name) 
                    
                    print(f"   - ℹ️ Employee '{original_name_from_result}' not found in sheet. Adding as new row.")
                    new_row_data = ["" for _ in headers]
                    
                    # מצא את עמודת השם והוסף את השם
                    name_col_index = col_map[master_name_col_header] - 1
                    new_row_data[name_col_index] = original_name_from_result.strip()
                    
                    # הוסף את השורה החדשה לגיליון
                    monthly_ws.append_row(new_row_data, value_input_option='USER_ENTERED')
                    
                    # עדכן את המיפויים הפנימיים שלנו כדי למצוא את השורה הבאה
                    row_num = len(all_data) + 1
                    all_data.append(new_row_data) # הוסף לגרסה המקומית של הנתונים
                    row_map[original_name_from_result] = row_num
                    if name_norm:
                        row_map_variants[name_norm] = (original_name_from_result, row_num)
                # --- סוף התיקון ---
                
                # Update actual hours column
                if row_num:
                    summary = result.get('report_summary', {})
                    hours_val = summary.get('total_presence_hours')
                    
                    if hours_val is not None:
                        col = col_map[TARGET_COLUMN_TITLE]
                        updates_to_send.append(gspread.Cell(row_num, col, str(hours_val)))
                    
            # Send all updates for this period in one batch
            if updates_to_send:
                monthly_ws.update_cells(updates_to_send, value_input_option='USER_ENTERED')
                print(f"✅ Google Sheets update complete for '{period_clean}'!")
                print(f"   - {matched_count} employees matched and updated")
                print(f"   - {unmatched_count} employees added as new rows")
                print(f"   - {len(updates_to_send)} cells updated total")
            else:
                print(f"✅ Google Sheets is already up to date for '{period_clean}'. No changes.")

    except Exception as e:
        print(f"❌ General error during Google Sheets update: {repr(e)}")
    