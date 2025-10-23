# report_parser.py
import os
import re
import json
import pdfplumber
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# --- OCR fallback ---
from ocr_extractor import extract_text_with_ocr

# =========================
#  Config & OpenAI client
# =========================
load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("Error: 'OPENAI_API_KEY' not found in .env file.")
client = OpenAI(api_key=API_KEY)

ALL_RESULTS = []

# =========================
#  Known Hebrew Keywords Context
# =========================
KEYWORDS_CONTEXT = """
Known Hebrew keywords by category:

1. פרטי עובד (Employee Details):
   - שם העובד, שם פרטי, שם משפחה, מספר עובד, מספר תעודת זהות, מזהה עובד, מזהה, מחלקה, תפקיד, מס' עובד, שם:, לשם העובד:, Employee Name:, Employee:, שם משפחה:, שם פרטי:

2. סיכום שעות (Hours Summary):
   - סה"כ שעות נוכחות, שעות עבודה, סה"כ שעות, שעות מאושרות, שעות לתשלום, שעות לתשלום כולל, סה"כ שעות לתשלום, סה"כ שעות מאושרות

3. שעות נוספות (Overtime):
   - שעות נוספות, שעות נוספות 125%, שעות נוספות 150%, סה"כ שעות נוספות

4. היעדרויות (Absences):
   - ימי חופשה, חופשה, ימי מחלה, מחלה, ימי חג, חג, חגים, היעדרות, היעדרויות

5. תקן (Norm/Quota):
   - תקן, שעות תקן, משרה, אחוז משרה

6. תאריכים ודוח (Dates/Report):
   - חודש, חודש הדוח, לתקופה, מתאריך, עד תאריך, תקופת הדוח, חודש עבודה, חודש שכר, שנת דוח, שנה

Use these keywords to help identify relevant fields in the report text.
"""

# =========================
#  Heuristics
# =========================
_HEB_CHARS = re.compile(r"[א-ת]")

def _looks_unreadable(txt: str) -> bool:
    """
    Heuristic to decide if PDF text is garbled/empty and needs OCR.
    Triggers OCR if:
      - Very short text, or
      - Contains very few Hebrew letters relative to length, or
      - Typical reversed/garbled artifacts (lots of punctuation with few letters)
    """
    t = (txt or "").strip()
    if len(t) < 80:
        return True
    heb = len(_HEB_CHARS.findall(t))
    # ratio of hebrew letters to total characters
    ratio = heb / max(len(t), 1)
    if ratio < 0.03:      # almost no Hebrew detected
        return True
    # Many colons/slashes and few letters is a common artifact
    punct = sum(t.count(x) for x in [":", "/", "\\", "*"])
    if punct > 200 and heb < 200:
        return True
    return False

# =========================
#  Extractors
# =========================
def _extract_text_from_pdf(file_path: str) -> str:
    """
    Extract text from a text-based PDF (no OCR).
    """
    text = ""
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                # Tight tolerances keep columns less jumbled in some reports
                page_txt = page.extract_text(x_tolerance=1, y_tolerance=1) or ""
                text += page_txt + "\n"
    except Exception as e:
        print(f"⚠️ Error reading PDF {file_path}: {e}")
    return text.strip()

def _extract_text_from_excel_or_csv(file_path: str) -> str:
    """
    Extracts all text from all sheets in an Excel/CSV into a single string.
    """
    text = ""
    p = Path(file_path)
    try:
        if p.suffix.lower() in [".xlsx", ".xls"]:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
                text += f"--- Sheet: {sheet_name} ---\n"
                text += df.fillna("").to_string(index=False, header=False) + "\n\n"
        elif p.suffix.lower() == ".csv":
            df = pd.read_csv(file_path, header=None, dtype=str)
            text += "--- CSV Content ---\n"
            text += df.fillna("").to_string(index=False, header=False) + "\n\n"
    except Exception as e:
        print(f"⚠️ Error reading Excel/CSV {file_path}: {e}")
    return text.strip()

# =========================
#  Helper for reversing Hebrew words if reversed
# =========================
def _fix_reversed_hebrew(text: str) -> str:
    def maybe_reverse(word):
        if re.fullmatch(r"[א-ת]{2,}", word) and len(word) > 2:
            return word[::-1]
        return word
    fixed_lines = []
    for line in text.splitlines():
        fixed_words = [maybe_reverse(w) for w in line.split()]
        fixed_lines.append(" ".join(fixed_words))
    return "\n".join(fixed_lines)

# =========================
#  Normalize terms
# =========================
def normalize_terms(text: str) -> str:
    """
    Replace similar terms with a unified term for 'סהכ שעות נוכחות'.
    """
    replacements = {
        r"סה\"כ שעות": "סהכ שעות נוכחות",
        r"סהכ שעות": "סהכ שעות נוכחות",
        r"נוכחות ברוטו": "סהכ שעות נוכחות",
        r"סה\"כ שעות נוכחות": "סהכ שעות נוכחות",
        r"סה\"כ שעות עבודה": "סהכ שעות נוכחות",
        r"סה\"כ שעות לתשלום": "סהכ שעות נוכחות",
        r"סה\"כ שעות מאושרות": "סהכ שעות נוכחות",
    }
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)
    return text

# =========================
#  New helper: convert time strings to decimal hours
# =========================
def time_to_decimal(t: str) -> float | None:
    """
    Convert a time string like '161:10' or '12:30' to decimal hours (e.g. 161.17 or 12.5).
    Returns None if format is invalid.
    """
    if not t or not isinstance(t, str):
        return None
    t = t.strip()
    # match HH:MM or H:MM
    m = re.fullmatch(r"(\d+):(\d{1,2})", t)
    if not m:
        try:
            # try float conversion directly
            return float(t)
        except:
            return None
    hours = int(m.group(1))
    minutes = int(m.group(2))
    if minutes >= 60:
        return None
    decimal = hours + minutes / 60
    return round(decimal, 2)

# =========================
#  New helper: pre-extract numeric summary from text
# =========================
def _preextract_numeric_summary(text: str) -> dict:
    """
    Scan text for numeric hour values (decimal or HH:MM) for keys of interest,
    keep the highest found value per key.
    Returns dict with keys matching LLM expected keys.
    """
    keys_map = {
        "סהכ שעות נוכחות": "total_presence_hours",
        "סהכ שעות": "total_presence_hours",
        "סהכ שעות עבודה": "total_presence_hours",
        "סהכ שעות לתשלום": "total_payable_hours",
        "סהכ שעות מאושרות": "total_approved_hours",
        "שעות לתשלום": "total_payable_hours",
        "שעות מאושרות": "total_approved_hours",
        "שעות נוספות": "overtime_hours",
        "חופשה": "vacation_days",
        "ימי חופשה": "vacation_days",
        "מחלה": "sick_days",
        "ימי מחלה": "sick_days",
        "חג": "holiday_days",
        "ימי חג": "holiday_days",
    }

    # Prepare regex to find lines with key and a number (decimal or HH:MM)
    # We'll look for lines containing a keyword and then a number nearby
    # number can be: digits with optional decimal, or digits:digits
    pattern = re.compile(
        r"(?P<key>" + "|".join(re.escape(k) for k in keys_map.keys()) + r")\s*[:\-]?\s*(?P<val>\d+(?:[:.]\d{1,2})?)"
    )

    found = {}
    for match in pattern.finditer(text):
        key = match.group("key")
        val_str = match.group("val")
        dec_val = time_to_decimal(val_str)
        if dec_val is None:
            continue
        mapped_key = keys_map[key]
        # keep max value found
        if mapped_key not in found or (found[mapped_key] is None) or (dec_val > found[mapped_key]):
            found[mapped_key] = dec_val

    return found

# =========================
#  LLM call
# =========================
def _analyze_text_with_llm(raw_text: str, file_name: str, hints: dict | None = None) -> dict | None:
    """
    Send the raw text to the model and get structured JSON back.
    Keep keys aligned with main.py expectations.
    Optionally include numeric hints as comments in prompt.
    """
    if not (raw_text or "").strip():
        print(f"⚠️ Skipping {file_name}, no text was extracted.")
        return None

    detected_name = None
    name_match = re.search(r"(?:שם\s*העובד|עובד|Employee Name|שם\s*:)\s*[:\-]?\s*([א-תA-Za-z'\-\" ]{3,40})", raw_text)
    if name_match:
        detected_name = name_match.group(1).strip().split("\n")[0]
        # Exclude common false positives
        if any(x in detected_name for x in ["דוח", "נוכחות", "חודש", "מערכת", "דו\"ח"]):
            detected_name = None

    # Fallback if not found
    if not detected_name:
        candidates = re.findall(r"([א-ת]{2,}\s+[א-ת]{2,}(?:\s+[א-ת]{2,})?)", raw_text[:400])
        for c in candidates:
            if not any(x in c for x in ["דוח", "נוכחות", "חודש", "מערכת", "דו\"ח"]):
                detected_name = c.strip()
                break

    # Fix reversed two-word Hebrew names
    if detected_name and re.fullmatch(r"[א-ת]{2,}\s+[א-ת]{2,}", detected_name):
        parts = detected_name.split()
        if all(re.fullmatch(r"[א-ת]{2,}", p) for p in parts):
            detected_name = " ".join(p[::-1] for p in parts)

    name_hint = f"\nDetected possible employee name (for reference): {detected_name}\n" if detected_name else ""

    hints_text = ""
    if hints:
        hints_lines = []
        for k, v in hints.items():
            if v is not None:
                hints_lines.append(f"- {k}: {v}")
        if hints_lines:
            hints_text = "\n\nNumeric hints extracted from report text:\n" + "\n".join(hints_lines) + "\n"

    prompt = f"""
You are an expert data extractor for Hebrew time-attendance reports.
Use the following list of known Hebrew keywords to identify relevant fields:
{KEYWORDS_CONTEXT}

When identifying "employee_name":
- Look for labels like "שם העובד", "עובד:", "Employee Name:", "שם:" or similar.
- Extract the 2–3 consecutive Hebrew or Latin words following that label.
- Ignore single Hebrew words unless directly connected to those labels.
- Prefer full names (first + last).
- If the extracted Hebrew name appears reversed (e.g., דבוע סיטרכ), reverse the letters to restore proper order (כריסטוב).
{name_hint}
{hints_text}
Given the following report text (Hebrew, may include tables), extract the fields below.
If a field is not present, return null for that field.
Return ONLY valid JSON (no comments, no markdown fences).

Required JSON keys (exactly these):
{{
  "employee_name": null,
  "employee_id": null,
  "employee_number": null,
  "report_month": null,
  "total_presence_hours": null,
  "total_approved_hours": null,
  "total_payable_hours": null,
  "overtime_hours": null,
  "vacation_days": null,
  "sick_days": null,
  "holiday_days": null
}}

Example output:
{{
  "employee_name": "ישראל כהן",
  "employee_id": "123456789",
  "employee_number": "98765",
  "report_month": "יוני 2023",
  "total_presence_hours": 160,
  "total_approved_hours": 155,
  "total_payable_hours": 150,
  "overtime_hours": 10,
  "vacation_days": 2,
  "sick_days": 1,
  "holiday_days": 0
}}

Text to analyze:
---
{raw_text}
---
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You extract precise structured data from Hebrew attendance reports. Output JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        response_text = response.choices[0].message.content.strip()
        # Strip possible fences if any
        cleaned = response_text.replace("```json", "").replace("```", "").strip()
        data = json.loads(cleaned)
        # minimal sanity: ensure all keys exist
        expected = [
            "employee_name","employee_id","employee_number","report_month",
            "total_presence_hours","total_approved_hours","total_payable_hours",
            "overtime_hours","vacation_days","sick_days","holiday_days"
        ]
        for k in expected:
            data.setdefault(k, None)
        return data
    except json.JSONDecodeError:
        print(f"❌ JSON Error: Model did not return valid JSON for {file_name}. Response snippet: {response_text[:300]!r}")
    except Exception as e:
        print(f"❌ API Error for {file_name}: {e}")
    return None

# =========================
#  Fix common names after LLM output
# =========================
def fix_common_names(data: dict) -> dict:
    """
    Fix common distorted or reversed names in LLM output.
    """
    name = data.get("employee_name")
    if not name:
        return data

    # Fix reversed two-word Hebrew names (again, just in case)
    if re.fullmatch(r"[א-ת]{2,}\s+[א-ת]{2,}", name):
        parts = name.split()
        if all(re.fullmatch(r"[א-ת]{2,}", p) for p in parts):
            fixed_name = " ".join(p[::-1] for p in parts)
            data["employee_name"] = fixed_name

    return data

# =========================
#  Public API
# =========================
def parse_report(file_path: str) -> dict:
    """
    Detect file type, extract text (with OCR fallback for PDFs), send to LLM,
    and return a dict aligned with main.py printing logic.
    """
    p = Path(file_path)
    file_name = p.name

    result = {
        "file": file_name,
        "employee_name": None,
        "employee_id": None,
        "report_period": None,    # maintained for main.py compatibility
        "report_summary": None    # will hold a dict of totals if present
    }

    # 1) Extract raw text
    if p.suffix.lower() == ".pdf":
        raw_text = _extract_text_from_pdf(file_path)
        if _looks_unreadable(raw_text):
            print(f"⚠️ {file_name}: PDF text unreadable — using OCR fallback.")
            raw_text = extract_text_with_ocr(file_path)
            # After OCR, run pre-extraction to help with scanned reports
            numeric_hints = _preextract_numeric_summary(raw_text)
        else:
            numeric_hints = _preextract_numeric_summary(raw_text)
    elif p.suffix.lower() in [".xlsx", ".xls", ".csv"]:
        raw_text = _extract_text_from_excel_or_csv(file_path)
        numeric_hints = _preextract_numeric_summary(raw_text)
    else:
        print(f"--- Skipping unsupported file type: {file_name} ---")
        return result

    raw_text = _fix_reversed_hebrew(raw_text)

    # Normalize terms before LLM call
    normalized_text = normalize_terms(raw_text)

    # Also pre-extract numeric hints from normalized text (in case normalization changed something)
    numeric_hints = _preextract_numeric_summary(normalized_text)

    # Convert any HH:MM values in numeric_hints to decimals (already done by time_to_decimal),
    # but just ensure integer values are floats for uniformity
    for k, v in numeric_hints.items():
        if v is not None:
            numeric_hints[k] = float(v)

    # 2) LLM analysis → structured JSON
    llm = _analyze_text_with_llm(normalized_text, file_name, hints=numeric_hints)
    if not llm:
        return result

    # Fix common names in LLM output
    llm = fix_common_names(llm)

    # 3) Map to expected shape for main.py
    result["employee_name"]  = llm.get("employee_name")
    result["employee_id"]    = llm.get("employee_id") or llm.get("employee_number")
    # prefer explicit month; keep compatibility with "report_period"
    result["report_period"]  = llm.get("report_month")

    # Build a lightweight summary dict (so main.py pretty-prints something useful)
    result["report_summary"] = {
        "total_presence_hours": llm.get("total_presence_hours"),
        "total_approved_hours": llm.get("total_approved_hours"),
        "total_payable_hours": llm.get("total_payable_hours"),
        "overtime_hours":      llm.get("overtime_hours"),
        "vacation_days":       llm.get("vacation_days"),
        "sick_days":           llm.get("sick_days"),
        "holiday_days":        llm.get("holiday_days"),
    }

    # Debug snippet if total_presence_hours is None
    if result["report_summary"].get("total_presence_hours") is None:
        snippet = normalized_text[:300].replace("\n", " ")
        print(f"⚠️ Debug: total_presence_hours is None for {file_name}. Text snippet: {snippet}")

    # --- Auto-export result to CSV for debugging/inspection ---
    try:
        out_folder = Path(file_path).parent
        base_name = Path(file_path).stem
        csv_path = out_folder / f"{base_name}_parsed.csv"

        # Write CSV
        import csv
        # Flatten dict for CSV: use top-level keys and, for report_summary, flatten its keys as columns
        csv_row = result.copy()
        summary = csv_row.pop("report_summary", {}) or {}
        for k, v in summary.items():
            csv_row[k] = v
        # Write header and row
        with open(csv_path, "w", encoding="utf-8", newline='') as cf:
            writer = csv.DictWriter(cf, fieldnames=list(csv_row.keys()))
            writer.writeheader()
            writer.writerow(csv_row)
        print(f"✅ Parsed result exported to CSV: {csv_path}")
    except Exception as e:
        print(f"⚠️ Failed to export parsed result for {file_path}: {e}")

    ALL_RESULTS.append(result)

    return result

# (No __main__ block – main.py runs this module.)

def export_all_results():
    """
    Export the entire ALL_RESULTS list into a single JSON and CSV file
    in the current working directory.
    """
    if not ALL_RESULTS:
        print("⚠️ No results to export in export_all_results().")
        return

    out_folder = Path.cwd()
    csv_path = out_folder / "all_reports_parsed.csv"

    try:
        # Write consolidated CSV
        import csv
        csv_rows = []
        for res in ALL_RESULTS:
            row = res.copy()
            summary = row.pop("report_summary", {}) or {}
            for k, v in summary.items():
                row[k] = v
            csv_rows.append(row)

        fieldnames = set()
        for row in csv_rows:
            fieldnames.update(row.keys())
        fieldnames = sorted(fieldnames)

        with open(csv_path, "w", encoding="utf-8", newline='') as cf:
            writer = csv.DictWriter(cf, fieldnames=fieldnames)
            writer.writeheader()
            for row in csv_rows:
                writer.writerow(row)
        print(f"✅ Consolidated parsed results exported to CSV: {csv_path}")

    except Exception as e:
        print(f"⚠️ Failed to export consolidated parsed results: {e}")