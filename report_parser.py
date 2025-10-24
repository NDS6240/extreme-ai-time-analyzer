import os
import re
import json
import pdfplumber
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

from ocr_extractor import extract_text_with_ocr
from dynamic_extractor import _preextract_dynamic_summary
from template_trainer import auto_detect_zones

load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("Error: 'OPENAI_API_KEY' not found in .env file.")
client = OpenAI(api_key=API_KEY)

ALL_RESULTS = []


# --------------------------
# Generic helpers
# --------------------------

def _looks_unreadable(txt: str) -> bool:
    if not txt or len(txt) < 80:
        return True
    heb = len(re.findall(r"[א-ת]", txt))
    return (heb / max(len(txt), 1)) < 0.03


def _extract_text_from_pdf(file_path: str) -> str:
    text = ""
    try:
        with pdfplumber.open(file_path) as pdf:
            for p in pdf.pages:
                text += (p.extract_text(x_tolerance=1, y_tolerance=1) or "") + "\n"
    except Exception as e:
        print(f"⚠️ Error reading PDF {file_path}: {e}")
    return text.strip()


def _extract_text_from_excel_or_csv(file_path: str) -> str:
    text = ""
    p = Path(file_path)
    try:
        if p.suffix.lower() in [".xlsx", ".xls"]:
            xls = pd.ExcelFile(file_path)
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
                text += df.fillna("").to_string(index=False, header=False) + "\n"
        elif p.suffix.lower() == ".csv":
            df = pd.read_csv(file_path, header=None, dtype=str)
            text += df.fillna("").to_string(index=False, header=False) + "\n"
    except Exception as e:
        print(f"⚠️ Error reading Excel/CSV {file_path}: {e}")
    return text.strip()


def _fix_reversed_hebrew(text: str) -> str:
    def flip(w): return w[::-1] if re.fullmatch(r"[א-ת]{2,}", w) else w

    return "\n".join(" ".join(flip(w) for w in line.split()) for line in text.splitlines())


def _normalize_hebrew_name(name: str) -> str:
    """Normalize names שנשלפו הפוכים (RTL)."""
    if not name:
        return name
    heb = re.findall(r"[א-ת]", name)
    if len(heb) < max(1, int(len(name) * 0.3)):
        return name
    parts = [p for p in re.split(r"[\s,;]+", name.strip()) if p]
    if len(parts) >= 2:
        def flip_token(tok: str) -> str:
            if re.fullmatch(r"[א-ת\"'־-]+", tok) and len(tok) > 1:
                return tok[::-1]
            return tok

        return " ".join(flip_token(p) for p in parts[::-1])
    return name


# --------------------------
# Zones (templates)
# --------------------------

def _zones_specific_path(file_path: str) -> Path:
    p = Path(file_path)
    return Path("zones_templates") / f"{p.stem}_zones.json"


def _load_zones_for_file(file_path: str) -> dict:
    """Loads zones JSON. Tries specific (<stem>_zones.json) then global (zones_template.json).
       Supports:
       A) { "page_1": {"employee_name_zone": {x0,top,x1,bottom}, "hours_table_zone": {...}}, ...}
       B) { "employee_name_zone": {"page":1,"bbox":[x0,top,x1,bottom]}, ... }
       Returns: { key: (page_index0, [x0,top,x1,bottom]) }
    """
    specific = _zones_specific_path(file_path)
    general = Path("zones_template.json")
    json_path = specific if specific.exists() else (general if general.exists() else None)
    if not json_path:
        print(f"⚠️ No zones found for {Path(file_path).name}")
        return {}
    print(f"🗂️ Using zones template: {json_path.name}")

    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"⚠️ Error loading zones JSON '{json_path}': {e}")
        return {}

    norm = {}

    # Shape A
    if isinstance(data, dict) and any(str(k).startswith("page_") for k in data.keys()):
        for page_key, zones in data.items():
            try:
                page_num = int(str(page_key).split("_")[-1])
            except Exception:
                page_num = 1
            page_idx = max(0, page_num - 1)
            for key in ("employee_name_zone", "hours_table_zone"):
                box = zones.get(key)
                if isinstance(box, dict) and all(k in box for k in ("x0", "top", "x1", "bottom")):
                    norm[key] = (page_idx, [float(box["x0"]), float(box["top"]),
                                            float(box["x1"]), float(box["bottom"])])
        return norm

    # Shape B
    for key in ("employee_name_zone", "hours_table_zone"):
        z = data.get(key)
        if isinstance(z, dict) and isinstance(z.get("bbox"), (list, tuple)):
            page_idx = max(0, int(z.get("page", 1)) - 1)
            bbox = [float(v) for v in z["bbox"][:4]]
            norm[key] = (page_idx, bbox)

    return norm


def _learn_zones_if_missing(file_path: str) -> None:
    """Self-learn: אם אין תבנית לקובץ — מזהה ושומר."""
    specific = _zones_specific_path(file_path)
    if specific.exists():
        return
    Path("zones_templates").mkdir(parents=True, exist_ok=True)
    try:
        print(f"🧩 Learning layout from new form: {Path(file_path).name}")
        detected = auto_detect_zones(file_path)
        if not detected:
            print("⚠️ auto_detect_zones returned nothing; skipping save.")
            return
        specific.write_text(json.dumps(detected, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"✅ Saved template: {specific}")
    except Exception as e:
        print(f"⚠️ Failed to learn zones for {file_path}: {e}")


def _extract_zone_texts(file_path: str, zones: dict) -> dict:
    out = {}
    if not zones:
        return out
    try:
        with pdfplumber.open(file_path) as pdf:
            for key, (page_idx, bbox) in zones.items():
                if 0 <= page_idx < len(pdf.pages):
                    page = pdf.pages[page_idx]
                    t = page.within_bbox(bbox).extract_text(x_tolerance=1, y_tolerance=1) or ""
                    out[key] = t.strip()
    except Exception as e:
        print(f"⚠️ Error extracting zone texts: {e}")
    return out


# --------------------------
# Hours extraction
# --------------------------

def _extract_standard_hours(text: str) -> dict:
    """Patterns for critical fields: 'שעות תקן' / 'שעות עבודה/נוכחות/בפועל' / 'שעות לתשלום' + triples."""
    res = {}
    if not text:
        return res

    def to_dec(s: str):
        m = re.fullmatch(r"(\d+):(\d{1,2})", s)
        if m:
            return round(int(m.group(1)) + int(m.group(2)) / 60, 2)
        try:
            return float(s)
        except Exception:
            return None

    patterns = [
        # שעות תקן
        (r"(?:סה[\"׳']?כ\s*)?שעות\s*תק[ן]\s*[:\-–]?\s*(\d+(?::\d{1,2})?|\d+\.\d+)", "שעות תקן"),
        # שעות עבודה/נוכחות/בפועל
        (r"(?:סה[\"׳']?כ\s*)?שעות\s*(?:נוכחות|עבודה|בפועל)\s*[:\-–]?\s*(\d+(?::\d{1,2})?|\d+\.\d+)", "שעות עבודה"),
        # שעות לתשלום / מאושרות לתשלום
        (r"(?:סה[\"׳']?כ\s*)?שעות\s*(?:לתשלום|מאושרות לתשלום)\s*[:\-–]?\s*(\d+(?::\d{1,2})?|\d+\.\d+)", "שעות לתשלום"),
    ]
    for rx, label in patterns:
        for m in re.finditer(rx, text):
            v = to_dec(m.group(1))
            if v is not None:
                res[label] = max(v, res.get(label, 0))

    # Triples: 160.00 159.25 159.24 (נוכחות/מאושרות/לתשלום)
    for m in re.finditer(r"(?<!\d)(\d+(?:\.\d+)?)[\s,]+(\d+(?:\.\d+)?)[\s,]+(\d+(?:\.\d+)?)(?!\d)", text):
        a, b, c = map(float, m.groups())
        presence = max(a, b, c)
        rest = sorted([a, b, c])
        res.setdefault("שעות עבודה", presence)
        res.setdefault("שעות מאושרות", rest[1])
        res.setdefault("שעות לתשלום", rest[0])
    return res


# --------------------------
# Minimal LLM for id/name/month
# --------------------------

def _detect_name_and_month(text: str) -> dict:
    prompt = f"""
    Extract only these fields from the following Hebrew text:
    employee_name, employee_id (או 'תעודת זהות'), and report_month (חודש/טווח).
    Return JSON only, with null if missing.
    ---
    {text[:2000]}
    ---
    """
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": "Extract only structured data as JSON."},
                      {"role": "user", "content": prompt}],
            temperature=0
        )
        raw = resp.choices[0].message.content.strip()
        raw = raw.replace("```json", "").replace("```", "")
        data = json.loads(raw)
        return {
            "employee_name": data.get("employee_name"),
            "employee_id": data.get("employee_id"),
            "report_month": data.get("report_month")
        }
    except Exception as e:
        print(f"⚠️ LLM name extraction failed: {e}")
        return {"employee_name": None, "employee_id": None, "report_month": None}


# --------------------------
# Main API
# --------------------------

def parse_report(file_path: str) -> dict:
    p = Path(file_path)
    file_name = p.name
    print(f"\n📄 Processing {file_name}")

    # Cache
    cache_path = p.with_suffix(".parsed.json")
    if cache_path.exists():
        print(f"💾 Using cached result for {file_name}")
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        ALL_RESULTS.append(data)
        return data

    # 1) Self-learn zones (no API cost)
    _learn_zones_if_missing(file_path)

    # 2) Load zones and read their text
    zones = _load_zones_for_file(file_path)
    zone_texts = _extract_zone_texts(file_path, zones) if zones else {}

    # 3) Get global text (for fallbacks)
    if p.suffix.lower() == ".pdf":
        raw_text = _extract_text_from_pdf(file_path)
        if _looks_unreadable(raw_text):
            print("🔍 Using OCR fallback...")
            raw_text = extract_text_with_ocr(file_path)
    elif p.suffix.lower() in [".xlsx", ".xls", ".csv"]:
        raw_text = _extract_text_from_excel_or_csv(file_path)
    else:
        print(f"⚠️ Unsupported file: {file_name}")
        return {}

    raw_text = _fix_reversed_hebrew(raw_text)

    # 4) Employee name — prefer zone, else LLM
    zone_name_text = (zone_texts.get("employee_name_zone") or "").strip()
    employee_name = None
    if zone_name_text:
        cleaned = re.sub(r"שם\s*העובד[:\-]?", "", zone_name_text)
        employee_name = _normalize_hebrew_name(cleaned.strip()) or None
        if employee_name:
            print(f"✅ Found employee name from zone: {employee_name}")

    id_data = {"employee_name": employee_name, "employee_id": None, "report_month": None}
    if not employee_name:
        # חיפוש fallback לשם עובד אם לא נמצא ב־zone
        m = re.search(r"(?:שם|עובד|עובדת)[:\s\-]+([א-ת\"׳'\s]+)", raw_text)
        if m:
            employee_name = _normalize_hebrew_name(m.group(1).strip())
        if employee_name:
            id_data["employee_name"] = employee_name
        else:
            id_data = _detect_name_and_month(raw_text)
            if id_data.get("employee_name"):
                id_data["employee_name"] = _normalize_hebrew_name(id_data["employee_name"]) or id_data["employee_name"]

    # 5) Hours extraction — zone first, then global
    dynamic_data = {}
    hours_zone_text = zone_texts.get("hours_table_zone") if zone_texts else None
    if hours_zone_text:
        dynamic_data.update(_preextract_dynamic_summary(hours_zone_text))
        dynamic_data.update(_extract_standard_hours(hours_zone_text))

    global_dynamic = _preextract_dynamic_summary(raw_text)
    for k, v in global_dynamic.items():
        dynamic_data.setdefault(k, v)

    for k, v in _extract_standard_hours(raw_text).items():
        dynamic_data.setdefault(k, v)

    # 6) Build result + cache
    result = {"file": file_name}
    result.update(id_data)
    if result.get("employee_name"):
        result["employee_name"] = _normalize_hebrew_name(result["employee_name"]) or result["employee_name"]
    result["report_summary"] = dynamic_data

    cache_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"💾 Cached: {cache_path}")
    ALL_RESULTS.append(result)
    return result
