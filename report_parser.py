import pdfplumber
import pandas as pd
import re
from pathlib import Path
from report_identifier import identify_report_type


def extract_total_hours_from_text(text: str) -> str | None:
    """
    Extracts the total hours string (e.g., 189:49 or 161:10) from a given text using regex.
    Returns the matched value or None if not found.
    """
    match = re.search(r"\b\d{1,3}[:.]\d{2}\b", text)
    if match:
        return match.group(0)
    return None


def parse_employee_report(file_path: str) -> str | None:
    """Parses an EMPLOYEE_REPORT PDF and extracts the total approved hours."""
    with pdfplumber.open(file_path) as pdf:
        text = ""
        for page in pdf.pages:
            text += page.extract_text() or ""

    # Look for keywords near total hours
    total_match = re.search(r"(?:(?:סה\"כ|Total).*?)(\d{1,3}[:.]\d{2})", text)
    if total_match:
        return total_match.group(1)

    # fallback simple extraction
    return extract_total_hours_from_text(text)


def parse_hilan_report(file_path: str) -> str | None:
    """Parses a HILAN_REPORT PDF and extracts total hours."""
    with pdfplumber.open(file_path) as pdf:
        text = ""
        for page in pdf.pages:
            text += page.extract_text() or ""

    total = extract_total_hours_from_text(text)
    return total


def parse_time_card_report(file_path: str) -> str | None:
    """Parses a TIME_CARD_REPORT PDF and extracts total presence hours."""
    with pdfplumber.open(file_path) as pdf:
        text = ""
        for page in pdf.pages:
            text += page.extract_text() or ""

    # Look for "סה\"כ נוכחות" or "Total presence"
    total_match = re.search(r"(?:סה\"כ\s*נוכחות|Total\s*Presence).*?(\d{1,3}[:.]\d{2})", text)
    if total_match:
        return total_match.group(1)
    return extract_total_hours_from_text(text)


def parse_excel_report(file_path: str) -> str | None:
    """Parses Excel timesheet and looks for any cell with total hours."""
    df = pd.read_excel(file_path, header=None)
    for _, row in df.iterrows():
        for cell in row:
            if isinstance(cell, str) and re.search(r"\d{1,3}[:.]\d{2}", cell):
                return cell
    return None


def parse_report(file_path: str) -> dict:
    """
    Detects the report type and extracts total hours.
    Returns a dictionary with report type and total hours.
    """
    report_type = identify_report_type(file_path)
    total_hours = None

    try:
        if report_type == "EMPLOYEE_REPORT":
            total_hours = parse_employee_report(file_path)
        elif report_type == "HILAN_REPORT":
            total_hours = parse_hilan_report(file_path)
        elif report_type == "TIME_CARD_REPORT":
            total_hours = parse_time_card_report(file_path)
        elif report_type == "EXCEL_REPORT":
            total_hours = parse_excel_report(file_path)
    except Exception as e:
        print(f"⚠️ Error parsing {file_path}: {e}")

    return {"file": Path(file_path).name, "type": report_type, "total_hours": total_hours}


if __name__ == "__main__":
    # Example test
    test_files = [
        "downloads/Report_Employee_2025-09.pdf",
        "downloads/דוח נוכחות ספטמבר.pdf",
        "downloads/Time Card Report 15_10_2025 08-57-45.pdf",
        "downloads/employeeReport_612781_1759655282.8953_662.xlsx",
    ]
    for f in test_files:
        print(parse_report(f))
