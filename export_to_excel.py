"""
Excel Export Module
Exports validated and unified employee attendance data to Excel files.
Uses master_employee.json for validation and matching.
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
import numpy as np
import difflib
from data_validator import validate_and_unify_data, load_master_data, generate_summary_table, export_summary_table


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


def export_summary_excel(all_results, employee_names=None):
    """
    Export validated and unified results to Excel.
    Now uses master_employee.json for validation and matching.
    """
    if not all_results:
        print("⚠️ No results to export to Excel.")
        return

    # Load master data
    load_master_data()
    
    # Validate and unify data using master list
    validated_results = validate_and_unify_data(all_results, log_unmatched=True)
    
    if not validated_results:
        print("⚠️ No validated results to export.")
        return

    # Flatten validated results
    flattened = []
    for item in validated_results:
        base = {k: v for k, v in item.items() if k != "report_summary"}
        summary = item.get("report_summary", {}) or {}

        # Ensure all keys exist even if report summary is empty
        base.update(summary)
        
        # Add validation metadata from data_validator
        if "company_name" in item:
            base["company_name"] = item.get("company_name", "")
        if "standard_hours" in item:
            base["standard_hours"] = item.get("standard_hours")
        if "reported_hours" in item:
            base["reported_hours"] = item.get("reported_hours")
        if "hours_status" in item:
            base["hours_status"] = item.get("hours_status", "")
        
        flattened.append(base)

    if not flattened:
        print("⚠️ No results to export to Excel (list was empty).")
        return

    df = pd.DataFrame(flattened)
    original_count = len(df)

    if original_count == 0:
        print("⚠️ No data in DataFrame to export.")
        return

    # Deduplication and normalization logic
    # Step 1: Normalize key fields for smart comparison with bidirectional name unification
    if employee_names:
        # Unify names against master list using fuzzy matching
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

    # Normalize employee IDs (convert None, '', and non-numeric values to NO_ID)
    df['norm_id'] = df['employee_id'].fillna('NO_ID').replace('', 'NO_ID').astype(str)
    df['norm_id'] = df['norm_id'].replace('None', 'NO_ID')

    # Step 2: Create quality score (completeness score)
    df['has_id'] = (df['norm_id'] != 'NO_ID').astype(int)

    hour_cols = ['total_presence_hours', 'total_approved_hours', 'total_payable_hours', 'overtime_hours',
                 'vacation_days', 'sick_days']
    # Ensure all hour columns exist before counting
    for col in hour_cols:
        if col not in df.columns:
            df[col] = np.nan

    df['hour_count'] = df[hour_cols].notna().sum(axis=1)

    # Step 3: Sort by quality: best row first, worst last
    df = df.sort_values(
        by=['norm_name', 'has_id', 'hour_count'],
        ascending=[True, False, False]
    )

    # Step 4: Remove duplicates intelligently (extra safety - validated_results should already be deduplicated)
    deduped_df = df.drop_duplicates(subset=['norm_name', 'norm_id'], keep='first')

    # Step 5: Clean up helper columns before export
    final_df = deduped_df.drop(columns=['norm_name', 'norm_id', 'has_id', 'hour_count'], errors='ignore')

    final_count = len(final_df)

    date_str = datetime.now().strftime("%Y-%m-%d")
    reports_folder = Path.cwd() / "downloads" / "reports"
    reports_folder.mkdir(parents=True, exist_ok=True)
    out_path = reports_folder / f"all_reports_summary_{date_str}.xlsx"

    # Export cleaned DataFrame to Excel
    final_df.to_excel(out_path, index=False, engine='openpyxl')

    print(f"✅ Excel summary exported: {out_path}")
    if (original_count > final_count):
        print(
            f"ℹ️ Deduplication removed {original_count - final_count} duplicate records (Original: {original_count}, Final: {final_count}).")
    
    # Generate and export summary table
    try:
        summary_df = generate_summary_table(validated_results)
        if not summary_df.empty:
            export_summary_table(summary_df)
            print("\n📊 Summary Table:")
            print(summary_df.to_string(index=False))
    except Exception as e:
        print(f"⚠️ Could not generate summary table: {repr(e)}")
