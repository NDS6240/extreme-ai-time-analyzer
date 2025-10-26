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
... [וכו']
"""

# =========================
#  Heuristics
# =========================
_HEB_CHARS = re.compile(r"[א-ת]")


def _looks_unreadable(txt: str) -> bool:
    """
    Heuristic to decide if PDF text is garbled/empty and needs OCR.
    """
    t = (txt or "").strip()
    if len(t) < 80:
        return True
    heb = len(_HEB_CHARS.findall(t))
    # ratio of hebrew letters to total characters
    ratio = heb / max(len(t), 1)
    if ratio < 0.03:  # almost no Hebrew detected
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
                # Increased tolerance slightly, might help with slightly offset text
                page_txt = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
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
#  Normalize terms
# =========================
def normalize_terms(text: str) -> str:
    """
    Replace similar terms with a unified term for 'סהכ שעות נוכחות'.
    """
    replacements = {
        r"סה\"כ שעות": "סהכ שעות נוכחות",
        r"סהכ שעות": "סהכ שעות נוכחות",
        r"נוכחות ברוטו": "סהכ שעות נוכחות",  # <-- This maps 'נוכחות ברוטו'
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
#  Pre-extract numeric summary
# =========================
def _preextract_numeric_summary(text: str) -> dict:
    """
    Scan text for numeric hour values (decimal or HH:MM) for keys of interest,
    keep the highest found value per key.
    Returns dict with keys matching LLM expected keys.
    """
    keys_map = {
        "תקן מחושב": "total_presence_hours",  # <-- Standard Hours
        "סהכ שעות נוכחות": "total_approved_hours",  # <-- Gross Presence (normalized)
        "סהכ שעות": "total_presence_hours",
        "סהכ שעות עבודה": "total_approved_hours",
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

    found = {}
    pattern_num = r"(\d+(?:[:.]\d{1,2})?)"

    for key, mapped_key in keys_map.items():
        if not key: continue
        found_values = []

        # 1. Look AFTER term (e.g., "Term: 160")
        try:
            regex_after = re.compile(re.escape(key) + r"[^0-9a-zA-Zא-ת]{0,10}" + pattern_num)
            for m in regex_after.finditer(text):
                val = time_to_decimal(m.group(1))
                if val is not None:
                    found_values.append(val)
        except Exception:
            pass

        # 2. Look BEFORE term (e.g., "160 :Term")
        try:
            regex_before = re.compile(pattern_num + r"[^0-9a-zA-Zא-ת]{0,10}" + re.escape(key))
            for m in regex_before.finditer(text):
                val = time_to_decimal(m.group(1))
                if val is not None:
                    found_values.append(val)
        except Exception:
            pass

        if found_values:
            current_max = found.get(mapped_key)
            new_max = max(found_values)
            if current_max is None or new_max > current_max:
                found[mapped_key] = new_max
    return found


# =========================
#  === Regex Name Finder (FIXED with Blacklist & Specific Patterns) ===
# =========================
def _find_name_with_regex(text: str) -> str | None:
    """Try to find the employee name using a series of robust regex patterns."""

    NAME_BLACKLIST = [
        "דוח", "דו\"ח", "נוכחות", "מערכת", "גליון", "מורחב", "כרטיס", "מספר", "דף",
        "אישור", "שעות", "סיכום", "טופס", "כניסה", "יציאה", "הערות", "בנק", "חברה",
        "בעמ", "בע\"מ", "גרופ", "אקסטרים", "לודן", "דיסקונט", "לישראל", "טפחות",
        "מזרחי", "לאומי", "חטיבת", "עובד", "עובדת", "מחלקה", "תאריך", "אגף", "אזור",
        "פרוייקטלים", "תוכנה", "יועץ", "קבלן", "סטטוס", "הגשה", "אושר", "תחילת",
        "עיבוד", "שוטף", "תחבורה", "ציבורית", "הסכם", "מפעל", "ניהול", "תצורה",
        "הפרשים", "אסמכת",
        # Reversed Junk (from logs)
        "ףד", "רפסמ", "תכרעמ", "סיטרכ", "דבוע", "חוד", "תועש", "רושיא", "אתכמסא"
    ]

    clean_text = text.replace('"', ' ').replace(',', ' ')

    # === Capture group: 2 or 3 Hebrew words, allowing "'-" ===
    # Word: [א-ת\'-]{2,}
    # Name: Word (\s+ Word){1,2} --> ensures 2 or 3 words
    name_capture_2_3_words = r"([א-ת\'-]{2,}(?:\s+[א-ת\'-]{2,}){1,2})"

    # === Single word capture (for partial matches) ===
    name_capture_1_word = r"([א-ת\'-]{2,})"


    patterns = [
        # --- Specific Patterns First ---
        # 1. Mini Grinblat pattern: "לעובד [Name] [ID]" (stricter capture)
        r"לעובד\s+([א-ת]{2,}\s+[א-ת]{2,})\s+\d{8,9}", # Expect exactly 2 words
        # 2. Anat Hazan-Yehuda pattern: "[ID] [Name]" (stricter capture)
        r"\d{8,9}\s+([א-ת]{2,}\s+[א-ת-]{2,})", # Expect 2 words, allow hyphen in second
        # 3. Haim Tirosh pattern: "[Name] [ID] עובד" (stricter capture)
        r"([א-ת]{2,}\s+[א-ת]{2,})\s+\d{5,}\s+עובד", # Expect exactly 2 words
        # 4. Sheri Avni pattern: "[Word] שם העובד : [Word]"
        r"([א-ת\'-]{2,})\s+שם העובד\s*:\s*([א-ת\'-]{2,})", # Special: 2 capture groups

        # --- General Patterns (using 2-3 word capture) ---
        # Key: Name
        r"(?:שם העובד|שם עובד|שם)[:\s]+" + name_capture_2_3_words,
        # עובד [ID] Name
        r"עובד\s+\d{6,9}\s+" + name_capture_2_3_words,
        # נוכחות לעובד Name (fallback, less strict than specific pattern 1)
        r"נוכחות\s+לעובד\s+" + name_capture_2_3_words,

        # --- LTR / Reversed Patterns ---
        # Name :Key
        name_capture_2_3_words + r"\s*:(?:שם העובד|שם עובד|עובד|שם)",

        # --- Partial Name Patterns (Last Resort - single word capture) ---
        # Key: Name (1 word)
        r"(?:שם העובד|שם עובד|שם)[:\s]+" + name_capture_1_word,
        # Name (1 word) :Key
        name_capture_1_word + r"\s*:(?:שם העובד|שם עובד|עובד|שם)",
        # [ID] Name (1 word)
        r"\d{8,9}\s+" + name_capture_1_word,
        # עובד [ID] Name (1 word)
        r"עובד\s+\d{6,9}\s+" + name_capture_1_word,
    ]

    # --- Iterate through patterns ---
    # Store potential matches and validate them *after* trying all patterns
    potential_matches = []

    for i, pattern in enumerate(patterns):
        try:
            # Use finditer to catch ALL matches for a pattern, not just the first
            for m in re.finditer(pattern, clean_text):
                # Special handling for Sheri Avni pattern
                if i == 3: # Index of the special pattern
                    # Ensure both groups were captured
                    if m.group(1) and m.group(2):
                        name = f"{m.group(1).strip()} {m.group(2).strip()}"
                        potential_matches.append(name)
                # Standard capture (always group 1 for others)
                elif m.group(1):
                    name = m.group(1).strip()
                    potential_matches.append(name)

        except re.error: continue
        except IndexError: continue # Should not happen with careful group indexing

    # --- Validate potential matches ---
    if not potential_matches:
        print(f"⚠️ Regex found no potential name matches for snippet: {clean_text[:100]}")
        return None

    # Sort matches (e.g., prefer longer names, or based on pattern index - earlier is better)
    # Simple sort: longest first
    potential_matches.sort(key=len, reverse=True)

    # Validate the best matches against the blacklist
    for name in potential_matches:
        if not name or len(name) < 2: continue

        name_parts = name.split()
        if not name_parts: continue

        is_valid_name = True
        for part in name_parts:
            # 1. Check blacklist
            if part in NAME_BLACKLIST:
                is_valid_name = False
                break
            # 2. Check if part is just numbers or junk
            if not re.search(r"[א-ת]", part):
                is_valid_name = False
                break
            # 3. Check length (avoid single letters unless it's like '-')
            if len(part) < 2 and part != '-':
                is_valid_name = False
                break

        if is_valid_name:
            print(f"✅ Regex selected valid name: '{name}'")
            return name # Return the first valid name found

    # If all potential matches failed validation
    print(f"⚠️ Regex found matches ({potential_matches}), but all were filtered by blacklist/validation.")
    return None


# =========================
#  LLM call (Modified with Blacklist)
# =========================
def _analyze_text_with_llm(raw_text: str, file_name: str, hints: dict | None = None,
                           name_hint: str | None = None) -> dict | None:
    """
    Send the raw text to the model and get structured JSON back.
    Now accepts a `name_hint` and uses the blacklist.
    """
    if not (raw_text or "").strip():
        print(f"⚠️ Skipping {file_name}, no text was extracted.")
        return None

    hints_text = ""
    if hints:
        hints_lines = []
        for k, v in hints.items():
            if v is not None:
                hints_lines.append(f"- {k}: {v}")
        if hints_lines:
            hints_text = "\n\nNumeric hints extracted from report text:\n" + "\n".join(hints_lines) + "\n"

    # --- New Name Hint Logic (with Blacklist) ---

    # === "מילון מוקשים" ל-LLM (subset for prompt clarity) ===
    blacklist_str = ", ".join([
        "דוח", "נוכחות", "מערכת", "גליון", "מורחב", "כרטיס", "מספר", "דף",
        "אישור", "שעות", "בנק", "חברה", "בעמ", "אקסטרים", "לודן", "דיסקונט",
        "עובד", "עובדת", "מחלקה", "תאריך", "אגף", "אזור", "ףד", "רפסמ", "תכרעמ", "דבוע ", "סיטרכ", "דבוע", "חוד",
        "אסמכת", "אתכמסא" # Added from logs
    ])

    name_instruction = ""
    if name_hint:
        name_instruction = f"""
When identifying "employee_name":
- A strong candidate name has been found: "{name_hint}"
- **USE THIS NAME.**
- Your job is to 1) Fix its RTL order if reversed, and 2) Complete it if it's partial.

- **CRITICAL: If the hint itself contains forbidden words like 'עובד', 'כרטיס', 'דוח', 'נוכחות', 'אסמכת', IGNORE THE HINT and find the real name yourself.** Forbidden words list: {blacklist_str}.
- **RTL FIX:** If the hint is reversed (e.g., "ןהכ לארשי" or "טלבנירג ינימ"), you must output the correct version (e.g., "ישראל כהן" or "מיני גרינבלט").
- **COMPLETION FIX:** If the hint is a single name (e.g., "אבני"), look in the text for the *other part* of the name nearby (e.g., "שרי" appears before "שם העובד : אבני"). The final name should be "שרי אבני".
- **LTR FIX:** If the hint is "תירוש חיים", it's LTR. Fix it to "חיים תירוש".
- If the hint is already correct (e.g., "אביה אלטויל", "ענת חזן-יהודה"), output it as is.
"""
    else:
        # If Regex failed, tell the LLM to find it (and avoid headers).
        name_instruction = f"""
When identifying "employee_name":
- **Regex failed.** You must find the name manually.
- Look for labels like "שם העובד", "עובד:", or a name near an ID number (e.g., "027321058 ענת חזן-יהודה").
- **CRITICAL: AVOID** extracting general titles, labels, or company names.
- **DO NOT EXTRACT THESE WORDS (THE "MINEFIELD"):** {blacklist_str}.
- Focus on actual human names (2-3 words).
- If the name you find is reversed (e.g., "ןהכ לארשי"), you must **fix it** to "ישראל כהן" in your JSON output.
"""
    # --- End New Name Logic ---

    prompt = f"""
You are an expert data extractor for Hebrew time-attendance reports.
Use the following list of known Hebrew keywords to identify relevant fields:
{KEYWORDS_CONTEXT}

{name_instruction}
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
                {"role": "system",
                 "content": "You extract precise structured data from Hebrew attendance reports. Output JSON only."},
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
            "employee_name", "employee_id", "employee_number", "report_month",
            "total_presence_hours", "total_approved_hours", "total_payable_hours",
            "overtime_hours", "vacation_days", "sick_days", "holiday_days"
        ]
        for k in expected:
            data.setdefault(k, None)
        return data
    except json.JSONDecodeError:
        print(
            f"❌ JSON Error: Model did not return valid JSON for {file_name}. Response snippet: {response_text[:300]!r}")
    except Exception as e:
        print(f"❌ API Error for {file_name}: {e}")
    return None


# =========================
#  Public API (Modified)
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
        "report_period": None,  # maintained for main.py compatibility
        "report_summary": None  # will hold a dict of totals if present
    }

    # 1) Extract raw text
    if p.suffix.lower() == ".pdf":
        raw_text = _extract_text_from_pdf(file_path)
        if _looks_unreadable(raw_text):
            print(f"⚠️ {file_name}: PDF text unreadable — using OCR fallback.")
            raw_text = extract_text_with_ocr(file_path)
    elif p.suffix.lower() in [".xlsx", ".xls", ".csv"]:
        raw_text = _extract_text_from_excel_or_csv(file_path)
    else:
        print(f"--- Skipping unsupported file type: {file_name} ---")
        return result

    # Normalize terms for numeric extraction
    normalized_text = normalize_terms(raw_text)
    numeric_hints = _preextract_numeric_summary(normalized_text)

    # Convert values to floats
    for k, v in numeric_hints.items():
        if v is not None:
            numeric_hints[k] = float(v)

    # === NEW: Try to find name with Regex FIRST ===
    # We use the *original* raw_text for this
    name_hint_from_regex = _find_name_with_regex(raw_text)
    # This print moved inside the function now

    # 2) LLM analysis → structured JSON
    llm = _analyze_text_with_llm(
        raw_text,
        file_name,
        hints=numeric_hints,
        name_hint=name_hint_from_regex
    )
    if not llm:
        return result

    # 3) Map to expected shape for main.py
    result["employee_name"] = llm.get("employee_name")
    result["employee_id"] = llm.get("employee_id") or llm.get("employee_number")
    result["report_period"] = llm.get("report_month")

    # Build a lightweight summary dict
    result["report_summary"] = {
        "total_presence_hours": llm.get("total_presence_hours"),
        "total_approved_hours": llm.get("total_approved_hours"),
        "total_payable_hours": llm.get("total_payable_hours"),
        "overtime_hours": llm.get("overtime_hours"),
        "vacation_days": llm.get("vacation_days"),
        "sick_days": llm.get("sick_days"),
        "holiday_days": llm.get("holiday_days"),
    }

    # --- Override LLM hour summary with pre-extracted hints if they exist ---
    for key in result["report_summary"].keys():
        if key in numeric_hints and numeric_hints[key] is not None:
            if result["report_summary"][key] is None or result["report_summary"][key] != numeric_hints[key]:
                result["report_summary"][key] = numeric_hints[key]

    # --- Auto-export result to CSV for debugging/inspection ---
    try:
        out_folder = Path(file_path).parent
        base_name = Path(file_path).stem
        # Avoid double extensions like .pdf_parsed.csv
        if base_name.endswith(p.suffix):
             base_name = base_name[:-len(p.suffix)]
        csv_path = out_folder / f"{base_name}_parsed.csv"


        import csv
        csv_row = result.copy()
        summary = csv_row.pop("report_summary", {}) or {}
        for k, v in summary.items():
            csv_row[k] = v

        # Ensure all expected keys exist for the DictWriter
        all_keys = list(result.keys()) + list(summary.keys())
        # Filter out 'report_summary' itself if it was somehow left
        all_keys = [k for k in all_keys if k != "report_summary"]
        all_keys = sorted(list(set(all_keys))) # Get unique sorted keys

        with open(csv_path, "w", encoding="utf-8", newline='') as cf:
            writer = csv.DictWriter(cf, fieldnames=all_keys)
            writer.writeheader()
            # Prepare row for writer, ensuring only keys in fieldnames are written
            row_to_write = {k: csv_row.get(k) for k in all_keys}
            writer.writerow(row_to_write)
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
        import csv
        csv_rows = []
        all_fieldnames = set()
        for res in ALL_RESULTS:
            row = res.copy()
            summary = row.pop("report_summary", {}) or {}
            for k, v in summary.items():
                row[k] = v
            csv_rows.append(row)
            all_fieldnames.update(row.keys())

        # Ensure consistent order based on the final set of all keys
        fieldnames = sorted(list(all_fieldnames))

        with open(csv_path, "w", encoding="utf-8", newline='') as cf:
            writer = csv.DictWriter(cf, fieldnames=fieldnames)
            writer.writeheader()
            for row in csv_rows:
                # Ensure only keys present in fieldnames are written
                row_to_write = {k: row.get(k) for k in fieldnames}
                writer.writerow(row_to_write)
        print(f"✅ Consolidated parsed results exported to CSV: {csv_path}")

    except Exception as e:
        print(f"⚠️ Failed to export consolidated parsed results: {e}")