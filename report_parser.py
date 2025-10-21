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
#  LLM call
# =========================
def _analyze_text_with_llm(raw_text: str, file_name: str) -> dict | None:
    """
    Send the raw text to the model and get structured JSON back.
    Keep keys aligned with main.py expectations.
    """
    if not (raw_text or "").strip():
        print(f"⚠️ Skipping {file_name}, no text was extracted.")
        return None

    prompt = f"""
You are an expert data extractor for Hebrew time-attendance reports.
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
    elif p.suffix.lower() in [".xlsx", ".xls", ".csv"]:
        raw_text = _extract_text_from_excel_or_csv(file_path)
    else:
        print(f"--- Skipping unsupported file type: {file_name} ---")
        return result

    # 2) LLM analysis → structured JSON
    llm = _analyze_text_with_llm(raw_text, file_name)
    if not llm:
        return result

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

    return result

# (No __main__ block – main.py runs this module.)