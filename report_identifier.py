import os
import pdfplumber


def identify_report_type(file_path: str) -> str:
    """Identify the type of report based on filename or content"""
    filename = os.path.basename(file_path).lower()

    # Step 1: detect by filename
    if "hilan" in filename or "נוכחות" in filename:
        return "HILAN_REPORT"
    if "report_employee" in filename:
        return "EMPLOYEE_REPORT"
    if "time card" in filename or "כרטיס עובד" in filename:
        return "TIME_CARD_REPORT"
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        return "EXCEL_REPORT"

    # Step 2: detect by content (for PDFs)
    if filename.endswith(".pdf"):
        try:
            with pdfplumber.open(file_path) as pdf:
                text = ""
                for page in pdf.pages[:2]:  # check first 2 pages only
                    text += page.extract_text() or ""

                text = text.lower()

                if "hilan" in text or "חילן" in text:
                    return "HILAN_REPORT"
                if "אישור שעות נוכחות" in text or "report employee" in text:
                    return "EMPLOYEE_REPORT"
                if "time card" in text or "כרטיס עובד" in text:
                    return "TIME_CARD_REPORT"
        except Exception as e:
            print(f"⚠️ Error reading {filename}: {e}")

    return "UNKNOWN"
