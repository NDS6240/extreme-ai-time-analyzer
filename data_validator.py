"""
Data Validation and Synchronization Module
Ensures employee hours are matched, verified, and synchronized correctly.
Uses master_employee.json and Google Sheet Employee Master as authoritative sources.
"""
import json
import difflib
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd


# Master data cache
_MASTER_DATA = None
_MASTER_EMPLOYEE_DICT = None  # {employee_name: {company_name, standard_hours}}


def load_master_data(master_file: str = "master_employee.json") -> Dict:
    """
    Load master employee data from JSON file.
    Returns dict with master_employees list.
    """
    global _MASTER_DATA, _MASTER_EMPLOYEE_DICT
    
    if _MASTER_DATA is not None:
        return _MASTER_DATA
    
    master_path = Path(master_file)
    if not master_path.exists():
        log_message(f"⚠️ Master file not found: {master_file}")
        return {"master_employees": []}
    
    try:
        with open(master_path, "r", encoding="utf-8") as f:
            _MASTER_DATA = json.load(f)
        
        # Build lookup dict for quick access
        _MASTER_EMPLOYEE_DICT = {}
        for emp in _MASTER_DATA.get("master_employees", []):
            name = emp.get("employee_name", "").strip()
            if name:
                _MASTER_EMPLOYEE_DICT[name] = {
                    "company_name": emp.get("company_name", ""),
                    "standard_hours": emp.get("standard_hours", 0.0)
                }
        
        log_message(f"✅ Loaded {len(_MASTER_EMPLOYEE_DICT)} employees from master file.")
        return _MASTER_DATA
    except Exception as e:
        log_message(f"❌ Error loading master file: {repr(e)}")
        return {"master_employees": []}


def get_master_employee_dict() -> Dict:
    """Get cached master employee dictionary."""
    if _MASTER_EMPLOYEE_DICT is None:
        load_master_data()
    return _MASTER_EMPLOYEE_DICT or {}


def normalize_name(name: str) -> str:
    """
    Normalize employee name for matching.
    Removes extra spaces, dashes, quotes.
    """
    if not name or not isinstance(name, str):
        return ""
    name = name.strip()
    name = re.sub(r'[-"\']', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def fix_rtl_name(name: str) -> str:
    """
    Simple RTL name fix - reverse if contains Hebrew.
    """
    if not name or not isinstance(name, str):
        return name
    if any('\u0590' <= c <= '\u05FF' for c in name):
        return name[::-1]
    return name


def match_employee_name(employee_name: str, master_names: Optional[List[str]] = None, 
                        threshold: float = 0.7) -> Tuple[Optional[str], float]:
    """
    Match employee name to master list using fuzzy matching.
    Returns (matched_name, match_ratio) or (None, 0.0) if no match found.
    
    Args:
        employee_name: Name to match
        master_names: List of master names (if None, uses loaded master data)
        threshold: Minimum similarity ratio (0-1)
    
    Returns:
        Tuple of (matched_name, match_ratio)
    """
    if not employee_name:
        return None, 0.0
    
    # Get master names if not provided
    if master_names is None:
        master_dict = get_master_employee_dict()
        master_names = list(master_dict.keys())
    
    if not master_names:
        return None, 0.0
    
    # Normalize input name
    name_norm = normalize_name(employee_name)
    
    # First check: exact match (case-insensitive, normalized)
    for master_name in master_names:
        master_norm = normalize_name(master_name)
        if name_norm.lower() == master_norm.lower():
            return master_name, 1.0
    
    # Second check: fuzzy match on normalized names
    best_match = None
    best_ratio = 0.0
    
    for master_name in master_names:
        master_norm = normalize_name(master_name)
        ratio = difflib.SequenceMatcher(None, name_norm.lower(), master_norm.lower()).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = master_name
    
    # Third check: try reversed name (RTL fix)
    if best_ratio < threshold:
        name_reversed = fix_rtl_name(name_norm)
        if name_reversed != name_norm:
            for master_name in master_names:
                master_norm = normalize_name(master_name)
                ratio = difflib.SequenceMatcher(None, name_reversed.lower(), master_norm.lower()).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = master_name
    
    # Return match only if above threshold
    if best_match and best_ratio >= threshold:
        return best_match, best_ratio
    
    return None, best_ratio


def validate_hours(reported_hours: Optional[float], standard_hours: float, 
                   tolerance_percent: float = 10.0) -> Tuple[bool, str]:
    """
    Validate reported hours against standard hours.
    
    Args:
        reported_hours: Hours reported by employee
        standard_hours: Standard hours from master data
        tolerance_percent: Allowed deviation percentage (default 10%)
    
    Returns:
        Tuple of (is_valid, status_message)
    """
    if reported_hours is None or standard_hours <= 0:
        return True, "No validation"  # Can't validate if missing data
    
    deviation = abs(reported_hours - standard_hours)
    deviation_percent = (deviation / standard_hours) * 100 if standard_hours > 0 else 0
    
    if deviation_percent <= tolerance_percent:
        return True, "OK"
    else:
        return False, f"⚠️ Irregular Hours ({deviation_percent:.1f}% deviation)"


def validate_and_unify_data(all_results: List[Dict], log_unmatched: bool = True) -> List[Dict]:
    """
    Validate and unify employee data from parsed reports.
    
    - Matches employee names to master list
    - Validates hours against standard_hours
    - Prevents duplicates
    - Adds validation metadata
    
    Args:
        all_results: List of parsed report results
        log_unmatched: Whether to log unmatched employees
    
    Returns:
        List of validated and unified results
    """
    if not all_results:
        return []
    
    master_dict = get_master_employee_dict()
    master_names = list(master_dict.keys())
    
    validated_results = []
    seen_names = set()  # For duplicate prevention
    
    log_message(f"\n{'='*60}")
    log_message(f"Starting data validation for {len(all_results)} reports")
    log_message(f"{'='*60}\n")
    
    unmatched_employees = []
    irregular_hours_count = 0
    
    for idx, result in enumerate(all_results, 1):
        employee_name = result.get("employee_name")
        if not employee_name:
            log_message(f"Report {idx}: No employee name found")
            continue
        
        report_summary = result.get("report_summary") or {}
        if not isinstance(report_summary, dict):
            report_summary = {}
        
        # Match employee name to master
        matched_name, match_ratio = match_employee_name(employee_name, master_names)
        
        if matched_name:
            # Get master data
            master_info = master_dict.get(matched_name, {})
            company_name = master_info.get("company_name", "")
            standard_hours = master_info.get("standard_hours", 0.0)
            
            # Normalize name for duplicate check
            norm_name = normalize_name(matched_name).lower()
            
            # Check for duplicates
            if norm_name in seen_names:
                log_message(f"⚠️ Report {idx}: Duplicate found for '{matched_name}' - merging data")
                # Find existing entry and merge
                for existing in validated_results:
                    if normalize_name(existing.get("employee_name", "")).lower() == norm_name:
                        # Merge report summaries
                        existing_summary = existing.get("report_summary", {})
                        new_summary = result.get("report_summary", {})
                        
                        # Keep higher value for each key
                        for key, new_val in new_summary.items():
                            if new_val is not None:
                                existing_val = existing_summary.get(key)
                                if existing_val is None or (isinstance(new_val, (int, float)) and new_val > existing_val):
                                    existing_summary[key] = new_val
                        break
                continue
            
            seen_names.add(norm_name)
            
            # Get reported hours (prefer total_presence_hours, fallback to total_approved_hours)
            reported_hours = report_summary.get("total_presence_hours") or report_summary.get("total_approved_hours")
            
            # Validate hours
            is_valid, status = validate_hours(reported_hours, standard_hours)
            if not is_valid:
                irregular_hours_count += 1
                log_message(f"⚠️ Report {idx}: Irregular hours for '{matched_name}': {reported_hours} vs {standard_hours} standard")
            
            # Create validated result
            validated_result = result.copy()
            validated_result["employee_name"] = matched_name  # Use matched name
            validated_result["company_name"] = company_name
            validated_result["standard_hours"] = standard_hours
            validated_result["reported_hours"] = reported_hours
            validated_result["hours_status"] = status
            validated_result["match_ratio"] = match_ratio
            
            validated_results.append(validated_result)
            
            log_message(f"✅ Report {idx}: '{employee_name}' → '{matched_name}' (match: {match_ratio:.2%}, status: {status})")
        else:
            # Unmatched employee
            unmatched_employees.append(employee_name)
            if log_unmatched:
                log_message(f"⚠️ Report {idx}: Unmatched employee '{employee_name}' (best match ratio: {match_ratio:.2%})")
            
            # Add to results with CHECK prefix
            validated_result = result.copy()
            validated_result["employee_name"] = f"**CHECK: {employee_name}"
            validated_result["company_name"] = ""
            validated_result["standard_hours"] = None
            validated_result["reported_hours"] = report_summary.get("total_presence_hours") or report_summary.get("total_approved_hours")
            validated_result["hours_status"] = "Unmatched"
            validated_result["match_ratio"] = match_ratio
            
            validated_results.append(validated_result)
    
    # Summary log
    log_message(f"\n{'='*60}")
    log_message(f"Validation Summary:")
    log_message(f"  - Total reports: {len(all_results)}")
    log_message(f"  - Matched employees: {len(validated_results) - len(unmatched_employees)}")
    log_message(f"  - Unmatched employees: {len(unmatched_employees)}")
    log_message(f"  - Irregular hours: {irregular_hours_count}")
    log_message(f"{'='*60}\n")
    
    return validated_results


def generate_summary_table(validated_results: List[Dict]) -> pd.DataFrame:
    """
    Generate summary table with employee data.
    
    Columns: Employee Name | Company | Standard Hours | Approved Hours | Difference | Status
    """
    if not validated_results:
        return pd.DataFrame()
    
    rows = []
    for result in validated_results:
        employee_name = result.get("employee_name", "")
        company_name = result.get("company_name", "")
        standard_hours = result.get("standard_hours")
        reported_hours = result.get("reported_hours")
        status = result.get("hours_status", "Unknown")
        
        # Calculate difference
        if standard_hours is not None and reported_hours is not None:
            difference = reported_hours - standard_hours
        else:
            difference = None
        
        rows.append({
            "Employee Name": employee_name,
            "Company": company_name,
            "Standard Hours": standard_hours,
            "Approved Hours": reported_hours,
            "Difference": difference,
            "Status": status
        })
    
    df = pd.DataFrame(rows)
    return df


def log_message(message: str, console: bool = True):
    """
    Log message to both console and log file.
    
    Args:
        message: Message to log
        console: Whether to also print to console
    """
    if console:
        print(message)
    
    # Ensure logs directory exists
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    log_file = logs_dir / "sync_log.txt"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        # Fallback if logging fails
        if console:
            print(f"⚠️ Failed to write to log file: {e}")


def export_summary_table(df: pd.DataFrame, output_dir: str = "downloads/reports_summary") -> Path:
    """
    Export summary table to Excel.
    
    Args:
        df: Summary DataFrame
        output_dir: Output directory path
    
    Returns:
        Path to exported file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"employee_hours_summary_{date_str}.xlsx"
    filepath = output_path / filename
    
    try:
        df.to_excel(filepath, index=False, engine='openpyxl')
        log_message(f"✅ Summary table exported: {filepath}")
        return filepath
    except Exception as e:
        log_message(f"❌ Error exporting summary table: {repr(e)}")
        raise


def apply_vacation_completion(all_results: List[Dict], master_dict: Dict) -> List[Dict]:
    """
    Ensure under-reported presence hours are supplemented by available vacation days.
    """
    HOURS_PER_VACATION_DAY = 8.6

    if not all_results or not isinstance(all_results, list):
        return all_results

    master_dict = master_dict or {}
    master_names = list(master_dict.keys())

    for result in all_results:
        if not isinstance(result, dict):
            continue

        report_summary = result.get("report_summary")
        if not isinstance(report_summary, dict):
            continue

        employee_name = result.get("employee_name")
        if not employee_name:
            continue

        matched_name, _ = match_employee_name(employee_name, master_names) if master_names else (None, 0.0)
        if matched_name:
            standard_hours = master_dict.get(matched_name, {}).get("standard_hours")
        else:
            standard_hours = None

        if not standard_hours or standard_hours <= 0:
            continue

        reported_hours = report_summary.get("total_presence_hours")
        vacation_days = report_summary.get("vacation_days")

        if (
            reported_hours is None
            or vacation_days is None
            or reported_hours >= standard_hours
            or vacation_days <= 0
        ):
            continue

        hours_needed = standard_hours - reported_hours
        vacation_hours_available = vacation_days * HOURS_PER_VACATION_DAY
        hours_to_use = min(hours_needed, vacation_hours_available)

        if hours_to_use <= 0:
            continue

        new_reported_hours = reported_hours + hours_to_use
        new_vacation_days = (vacation_hours_available - hours_to_use) / HOURS_PER_VACATION_DAY

        report_summary["original_presence_hours"] = reported_hours
        report_summary["auto_completed_hours"] = round(hours_to_use, 2)
        report_summary["total_presence_hours"] = round(new_reported_hours, 2)
        report_summary["vacation_days"] = round(new_vacation_days, 2)

        log_message(
            f"🔄 Vacation completion applied for '{employee_name}' — added {hours_to_use:.2f} hours, remaining vacation days: {new_vacation_days:.2f}"
        )

    return all_results
