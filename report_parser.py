import os
import re
import json
import difflib
import pdfplumber
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
import time 

# --- OCR fallback ---
from ocr_extractor import extract_text_with_ocr

# --- Google Sheets Imports for Name List ---
try:
    # אנו מייבאים את הפונקציות והמשתנים הדרושים מהקובץ הקיים
    from google_sheets_updater import authenticate, MASTER_SHEET_URL
    import gspread
    GSPREAD_AVAILABLE = True # נשאר כדי לא לשבור ייבוא אחר
except ImportError:
    print("⚠️ Could not import Google Sheets modules. Name list will rely on local 'terms_dictionary.json' only.")
    GSPREAD_AVAILABLE = False

# --- חדש: ייבוא ספריית Fuzzy Matching ---
try:
    from thefuzz import process as fuzz_process
    FUZZ_AVAILABLE = True
except ImportError:
    print("⚠️ WARNING: Library 'thefuzz' not found. Name hinting from email will be less accurate.")
    print("Please run: pip install thefuzz")
    FUZZ_AVAILABLE = False
# --- סוף קטע חדש ---


# =========================
#  Config & OpenAI client
# =========================
load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("Error: 'OPENAI_API_KEY' not found in .env file.")
client = OpenAI(api_key=API_KEY)

# --- DEBUG Flag ---
# Set DEBUG=true in .env file to enable verbose logging
DEBUG = os.getenv("DEBUG", "False").lower() == "true"

ALL_RESULTS = []

# =========================
#  --- הרשימה הסופית (Single Source of Truth) ---
# =========================
# כל הלוגיקה של טעינת שמות מגוגל שיטס וקבצים הוסרה.
# זו הרשימה היחידה שהמערכת תכיר.
EMPLOYEE_NAMES = [
    "מוריה פיטוסי", "אור כהן", "מאיה פוזנרסון", "שמואל קריסי", "מינה גרינבלט",
    "אברהם שורין", "מיכאל סטולברג", "בני בלאושטיין", "אפרת כהן", "זיוה אזולאי",
    "אלה קוגן", "שרית חודטוב", "נורית מור", "גלעד ספטי", "אלכסנדר גופמן",
    "עומר כהן", "קרן וויגלמן", "דניאלה ליבמן", "תומר ריידר", "מריוס קולנברג",
    "פול מלטסב", "דויד קליין", "שרון סדרייב", "איליה פינקלשטיין", "יניב עמרמי",
    "אלון גרוס", "ריקי קורלנסקי", "ישראל פרידמן", "אנטון אבולניקוב", "בוריס מוראדוב",
    "אוריאל אבני", "אורי רדו", "וסילי בוגייבסקי", "זויה ציבולסקיה", "חן שחר כהן",
    "יונתן מיימון", "לילך מקייטן", "מיכל פרץ", "מאיר שמייב", "דוראל בן חמו",
    "אבי כתב", "אבי מוטובה", "אביה אלטויל", "אברהם כהנא", "אודה-ליה ורבנר",
    "אלכסנדר בורדו", "אלכסנדר זברודסקי", "אנתוני אביב לוי", "אפרת מרקוביץ",
    "בן מרחוזי", "דני קון", "הילה בראון", "ולדימיר סוסקין", "זוהר לנגבורד",
    "חגי שגיא", "חיים תירוש", "יוסי מלכי", "מור גורן מישרין", "מילנה איזמאילוב",
    "מעיין מיטל", "מרגריטה פודגור", "מרום טואיטו", "ניב נתנאל", "נעה שנפ",
    "נעם טבק", "נעם לבינסקי", "נריה סבן", "סול מיכאלי", "סיון לנגר מרום",
    "סלבה בייטמן", "עדי סולומון", "ענת חזן", "שחר חברוני", "שירן צדקה",
    "שלמה ברנד", "שרה לוי", "שרי אבני", "אמיר זחאלקה", "ויקטוריה ארליכמן",
    "זאב סוסקין", "ליאור הס", "מנחם מאור עבוש", "ניר דיאמנט", "סמואל איסנין",
    "קיריל מילטיצקי", "שרית"
]
print(f"✅ Loaded {len(EMPLOYEE_NAMES)} names from definitive master list.")
# =========================
#  סוף הרשימה
# =========================


# =========================
#  Known Hebrew Keywords Context
# =========================
KEYWORDS_CONTEXT = """
Known Hebrew keywords by category:
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
    if len(t) < 30:
        return True
    heb = len(_HEB_CHARS.findall(t))
    # ratio of hebrew letters to total characters
    ratio = heb / max(len(t), 1)
    if ratio < 0.005:  # almost no Hebrew detected
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


def _fix_rtl_name(name: str) -> str:
    """
    Simple RTL name fix - checks against employee list and returns reversed name if closer match.
    Returns the name with characters in correct order.
    Checks against employee list - if reversed version is closer match, returns that.
    """
    if not name or not isinstance(name, str):
        return name
    
    # Only if name contains Hebrew characters
    if not any('\u0590' <= c <= '\u05FF' for c in name):
        return name
    
    # If employee list exists, check which version is closer
    if EMPLOYEE_NAMES:
        name_norm = name.strip()
        name_reversed = name[::-1].strip()
        
        # Check with difflib - which version is closer to employee list
        best_match_original = None
        best_ratio_original = 0.0
        best_match_reversed = None
        best_ratio_reversed = 0.0
        
        for master_name in EMPLOYEE_NAMES:
            master_norm = str(master_name).strip()
            
            # Check original version
            ratio_orig = difflib.SequenceMatcher(None, name_norm.lower(), master_norm.lower()).ratio()
            if ratio_orig > best_ratio_original:
                best_ratio_original = ratio_orig
                best_match_original = master_name
            
            # Check reversed version
            ratio_rev = difflib.SequenceMatcher(None, name_reversed.lower(), master_norm.lower()).ratio()
            if ratio_rev > best_ratio_reversed:
                best_ratio_reversed = ratio_rev
                best_match_reversed = master_name
        
        # If reversed version is closer (and difference is significant), use it
        if best_ratio_reversed > best_ratio_original + 0.1:  # Difference of at least 0.1
            return name_reversed
    
    # Without employee list or without significant difference - keep original name
    return name


def _apply_sanity_checks(report_summary: dict) -> dict:
    """
    Performs sanity checks on numeric data.
    If total_presence_hours exceeds 350 (maximum hours per month), resets the value and moves it to sanity_failed_minutes.
    """
    if not report_summary or not isinstance(report_summary, dict):
        return report_summary
    
    # Check total_presence_hours
    total_hours = report_summary.get('total_presence_hours')
    if total_hours is not None:
        try:
            hours_value = float(total_hours)
            if hours_value > 350:  # More than 350 hours per month - not reasonable
                # Move value to sanity_failed_minutes (in minutes)
                minutes_value = int(hours_value * 60)
                report_summary['sanity_failed_minutes'] = minutes_value
                # Reset total_presence_hours
                report_summary['total_presence_hours'] = None
                if DEBUG:
                    print(f"⚠️ Sanity check failed: total_presence_hours={hours_value} > 350, moved to sanity_failed_minutes={minutes_value}")
        except (ValueError, TypeError):
            pass  # Not a number - ignore
    
    return report_summary


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
        # שעות נוכחות
        "תקן מחושב": "total_presence_hours",
        "סהכ שעות נוכחות": "total_presence_hours",
        "שעות בפועל": "total_presence_hours",
        "נוכחות ברוטו": "total_presence_hours",
        "סהכ שעות": "total_presence_hours",
        "סהכ שעות עבודה": "total_presence_hours",
        "סה\"כ שעות נוכחות": "total_presence_hours",
        "סה\"כ שעות": "total_presence_hours",
        
        # שעות מאושרות
        "סהכ שעות מאושרות": "total_approved_hours",
        "שעות מאושרות": "total_approved_hours",
        "סה\"כ שעות מאושרות": "total_approved_hours",
        
        # שעות לתשלום
        "סהכ שעות לתשלום": "total_payable_hours",
        "שעות לתשלום": "total_payable_hours",
        "סה\"כ שעות לתשלום": "total_payable_hours",
        "סהכ 125%": "total_payable_hours",
        "סה\"כ 125%": "total_payable_hours",
        
        # שעות נוספות
        "שעות נוספות": "overtime_hours",
        "סהכ נוספות": "overtime_hours",
        "סה\"כ נוספות": "overtime_hours",
        "125%": "overtime_hours",
        "150%": "overtime_hours",
        
        # חופשה
        "חופשה": "vacation_days",
        "ימי חופשה": "vacation_days",
        "חופש": "vacation_days",
        
        # מחלה
        "מחלה": "sick_days",
        "ימי מחלה": "sick_days",
        
        # חג
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

    # --- Uses global EMPLOYEE_NAMES list (loaded once) ---

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
    name_capture_2_3_words = r"([א-ת\'-]{2,}(?:\s+[א-ת\'-]{2,}){1,2})"

    # === Single word capture (for partial matches) ===
    name_capture_1_word = r"([א-ת\'-]{2,})"

    patterns = [
        # --- Specific Patterns First ---
        # 1. Mini Grinblat pattern: "לעובד [Name] [ID]" (stricter capture)
        r"לעובד\s+([א-ת]{2,}\s+[א-ת]{2,})\s+\d{8,9}",  # Expect exactly 2 words
        # 2. Anat Hazan-Yehuda pattern: "[ID] [Name]" (stricter capture)
        r"\d{8,9}\s+([א-ת]{2,}\s+[א-ת-]{2,})",  # Expect 2 words, allow hyphen in second
        # 3. Haim Tirosh pattern: "[Name] [ID] עובד" (stricter capture)
        r"([א-ת]{2,}\s+[א-ת]{2,})\s+\d{5,}\s+עובד",  # Expect exactly 2 words
        # 4. Sheri Avni pattern: "[Word] שם העובד : [Word]"
        r"([א-ת\'-]{2,})\s+שם העובד\s*:\s*([א-ת\'-]{2,})",  # Special: 2 capture groups

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
    potential_matches = []

    for i, pattern in enumerate(patterns):
        try:
            for m in re.finditer(pattern, clean_text):
                # Special handling for Sheri Avni pattern
                if i == 3:  # Index of the special pattern
                    if m.group(1) and m.group(2):
                        name = f"{m.group(1).strip()} {m.group(2).strip()}"
                        potential_matches.append(name)
                # Standard capture (always group 1 for others)
                elif m.group(1):
                    name = m.group(1).strip()
                    potential_matches.append(name)

        except re.error:
            continue
        except IndexError:
            continue

        # --- Validate potential matches ---
    if not potential_matches:
        if DEBUG: print(f"ℹ️ Regex found no potential name matches for snippet: {clean_text[:100]}")
        # Do not return yet, try backup list
    else:
        potential_matches.sort(key=len, reverse=True)

        # Validate the best matches against the blacklist
        for name in potential_matches:
            if not name or len(name) < 2: continue

            name_parts = name.split()
            if not name_parts: continue

            is_valid_name = True
            for part in name_parts:
                if part in NAME_BLACKLIST:
                    is_valid_name = False
                    break
                if not re.search(r"[א-ת]", part):
                    is_valid_name = False
                    break
                if len(part) < 2 and part != '-':
                    is_valid_name = False
                    break

            if is_valid_name:
                # Simple RTL fix - use _fix_rtl_name function
                fixed_name = _fix_rtl_name(name)
                if DEBUG and fixed_name != name:
                    print(f"💡 Fixed RTL name: '{name}' -> '{fixed_name}'")
                if DEBUG: print(f"✅ Regex selected valid name: '{fixed_name}'")
                return fixed_name  # Return the first valid name found (primary success)

    # If all potential matches failed validation (or none found)
    if DEBUG: print(f"ℹ️ Regex failed validation or found no matches. Trying backup list...")

    # === Backup: Try to match with employee_names list using close matching ===
    # Only if regex failed to find a valid name
    if EMPLOYEE_NAMES:  # Use the global list
        two_word_pat = re.compile(r"([א-ת\'-]{2,}\s+[א-ת\'-]{2,})")
        three_word_pat = re.compile(r"([א-ת\'-]{2,}\s+[א-ת\'-]{2,}\s+[א-ת\'-]{2,})")
        candidates = set()
        for m in three_word_pat.finditer(clean_text):
            candidates.add(m.group(1).strip())
        for m in two_word_pat.finditer(clean_text):
            candidates.add(m.group(1).strip())

        for cand in sorted(candidates, key=len, reverse=True):
            is_blacklisted = False
            for part in cand.split():
                if part in NAME_BLACKLIST:
                    is_blacklisted = True
                    break
            if is_blacklisted:
                continue

                # Use global list and new cutoff=0.8
            matches = difflib.get_close_matches(cand, EMPLOYEE_NAMES, n=1, cutoff=0.8)
            if matches:
                match = matches[0]
                # Simple RTL fix - use _fix_rtl_name function
                fixed_match = _fix_rtl_name(match)
                if DEBUG and fixed_match != match:
                    print(f"💡 Fixed RTL name: '{match}' -> '{fixed_match}'")
                if DEBUG: print(f"✅ Found match from employee_names list (backup): {fixed_match}")
                return fixed_match  # Return backup match

    # All methods failed
    print(f" :-( All name extraction methods (Regex + Backup List) failed.")
    return None


# =========================
#  שדרוג: חיפוש שם חכם (Fuzzy) במטא-דאטה של המייל
# =========================
def _find_name_from_email_metadata(metadata: dict, employee_list: list) -> str | None:
    """
    Searches email metadata (subject, from, body) for a name from the employee_list
    using fuzzy matching.
    """
    if not metadata or not employee_list or not FUZZ_AVAILABLE:
        if FUZZ_AVAILABLE == False and DEBUG:
            print("  DEBUG: 'thefuzz' library not loaded, skipping fuzzy metadata search.")
        return None

    # איחוד כל הטקסט מהמטא-דאטה למחרוזת חיפוש אחת
    subject = metadata.get("subject", "")
    from_ = metadata.get("from", "")
    body_snippet = metadata.get("body_snippet", "")
    
    search_text = f"{subject} {from_} {body_snippet}".replace(">", " ").replace("<", " ").replace("[mailto:", " ")
    
    if DEBUG:
        print(f"  DEBUG: Searching for name in email metadata...")
        print(f"  DEBUG: Search Text Snippet: {search_text[:250]}...")

    # --- שימוש ב-Fuzzy Matching ---
    # מחלץ את ההתאמה הטובה ביותר מתוך הטקסט
    # score_cutoff=90 דורש התאמה גבוהה מאוד (למשל "מיני גרינבלט" יתאים ל"מינה גרינבלט")
    best_match = fuzz_process.extractOne(search_text, employee_list, score_cutoff=90)
    
    if best_match:
        # best_match הוא (שם, ציון)
        matched_name = best_match[0]
        if DEBUG: print(f"  ✅ Found name hint from Email Metadata: '{matched_name}' (Score: {best_match[1]})")
        return matched_name

    # --- אם נכשל, נסה בדיקה פשוטה (fallback) ---
    # זה יתפוס שמות שהם חלק מהרשימה (כמו "אמיר זחאלקה" שמופיע במדויק)
    for name in sorted(employee_list, key=len, reverse=True):
        if not name or len(name) < 5: 
            continue
        if name in search_text:
            if DEBUG: print(f"  ✅ Found name hint from Email Metadata (Simple Check): '{name}'")
            return name
    # --- סוף קטע ---

    if DEBUG: print(f"  ℹ️ No name hint found in email metadata.")
    return None


# =========================
#  LLM call (Modified with Blacklist)
# =========================
def _analyze_text_with_llm(raw_text: str, file_name: str, hints: dict | None = None,
                           name_hint_regex: str | None = None, 
                           name_hint_email: str | None = None) -> dict | None: # <-- שדות חדשים
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
            hints_text = "\n\nNumeric hints extracted from report text (use for guidance):\n" + "\n".join(
                hints_lines) + "\n"

    # --- New Name Hint Logic (with Blacklist) ---
    blacklist_str = ", ".join([
        "דוח", "נוכחות", "מערכת", "גליון", "מורחב", "כרטיס", "מספר", "דף",
        "אישור", "שעות", "בנק", "חברה", "בעמ", "אקסטרים", "לודן", "דיסקונט",
        "עובד", "עובדת", "מחלקה", "תאריך", "אגף", "אזור", "ףד", "רפסמ", "תכרעמ", "דבוע ", "סיטרכ", "דבוע", "חוד",
        "אסמכת", "אתכמסא"
    ])

    # --- New: Add known names list instruction (as requested) ---
    known_names_instruction = ""
    if EMPLOYEE_NAMES:
        # Create a comma-separated string, limit to ~500 chars for prompt safety
        names_str = ", ".join(EMPLOYEE_NAMES)
        if len(names_str) > 500:
            names_str = names_str[:500] + "..."
        known_names_instruction = f"""
When identifying "employee_name", you can also use this list of known employee names as a reference.
The name in the text might be a close match, not an exact one.
Known names: [{names_str}]
"""

    # --- START OF EDITED SECTION 1: name_instruction (עם עדיפות לרמז מהמייל) ---
    name_hint_regex_to_use = name_hint_regex if name_hint_regex else "None"
    name_hint_email_to_use = name_hint_email if name_hint_email else "None"

    name_instruction = f"""
**CRITICAL RULES FOR "employee_name":**

1.  **CHECK FOR REVERSED NAMES (RTL FIX):**
    - You **MUST** check if the name is reversed (e.g., "ןהכ לארשי" or "טלבנירג ינימ").
    - If it is reversed, you **MUST** fix it to the correct RTL order in your JSON output (e.g., "ישראל כהן" or "מינה גרינבלט"). This is a critical failure if missed.

2.  **AVOID THE "MINEFIELD" (Blacklist):**
    - You **MUST NOT** extract these words as a name: {blacklist_str}.

3.  **USE THE HINTS (Email has PRIORITY):**
    - **Hint 1 (From Email Metadata - HIGH PRIORITY):** "{name_hint_email_to_use}"
    - **Hint 2 (From File Content Regex - LOW PRIORITY):** "{name_hint_regex_to_use}"
    - The filename is: "{file_name}".

    - **Your Priority:**
        a) If **Hint 1** ("{name_hint_email_to_use}") is valid (not "None" and not in the MINEFIELD), **USE IT**. (And fix it if it's reversed).
        b) If **Hint 1** is invalid, check if **Hint 2** ("{name_hint_regex_to_use}") is valid (not "None" and not in the MINEFIELD). If yes, **USE IT**. (And fix it if it's reversed).
        c) If both hints are invalid, search the text body for labels like "שם העובד" or "עובד:".
        d) If you still can't find a name, **check the filename "{file_name}"** for a human name (e.g., "חיים תירוש").

4.  **FINAL CHECK:** The name must be a human name, not a label.
"""
    # --- END OF EDITED SECTION 1 ---

    # --- START OF EDITED SECTION 2: flexibility_instruction (ללא שינוי) ---
    flexibility_instruction = """
**CRITICAL RULES FOR NUMERIC DATA:**

Your main goal is to find all numeric totals. The reports are inconsistent. You must be a detective.

1.  **REQUIRED HOURS (Find these values):**
    - `total_presence_hours`: Find the main presence hours. Look for terms like "סהכ שעות נוכחות", "נוכחות ברוטו", "סהכ שעות".
    - `total_approved_hours`: Find the approved hours. Look for terms like "סהכ שעות מאושרות", "שעות מאושרות".
    - `total_payable_hours`: Find the payable hours. Look for terms like "סהכ שעות לתשלום", "שעות לתשלום".
    - `overtime_hours`: Find overtime. Look for "שעות נוספות", "סהכ נוספות", "125%", "150%". Sum them up if they are separate.

2.  **REQUIRED DAYS (Find these values):**
    - `vacation_days`: Look for "ימי חופשה", "חופשה".
    - `sick_days`: Look for "ימי מחלה", "מחלה".
    - `holiday_days`: Look for "ימי חג", "חג".

3.  **CRITICAL SANITY CHECK: Days vs. Hours vs. Minutes:**
    - You **MUST** distinguish between units.
    - **DAYS (ימים):** For `vacation_days`, `sick_days`, `holiday_days`, the value should be small (e.g., 1, 2.5, 7).
    - **PROBLEM EXAMPLE:** If you see "ימי מחלה: 483" or "מחלה: 483", this is **WRONG**. 483 is clearly **MINUTES** or a different field ID.
    - **YOUR ACTION:** Do **NOT** put 483 in `sick_days`. If you are sure it's minutes, create a *new key* `sick_minutes: 483`. If you are unsure, leave `sick_days` as `null`.
    - **HOURS (שעות):** For hour fields, the value is usually larger (e.g., 160, 10.5).

4.  **CAPTURE EVERYTHING ELSE (Be Flexible):**
    - After you secure the required keys, find **ALL OTHER** numeric summaries.
    - Examples: "ימי הבראה", "שעות כוננות", "מילואים", "בונוס שעות", "סהכ שעות חריגות".
    - Create *new keys* for them in the JSON output (e.g., `recuperation_days: 5`, `standby_hours: 10.5`, `military_days: 3`, `bonus_hours: 10`).
    - **DO NOT IGNORE DATA** just because it's not in the required list.
"""
    # --- END OF EDITED SECTION 2 ---

    # --- START OF EDITED SECTION 3: Main prompt ---
    prompt = f"""
The file being analyzed is named: "{file_name}"

You are an expert data extractor for Hebrew time-attendance reports. You are precise, methodical, and rigid. You follow all rules exactly.
Use the following list of known Hebrew keywords as a *guide*, not a strict list:
{KEYWORDS_CONTEXT}

{known_names_instruction}
{name_instruction}
{hints_text}
{flexibility_instruction}

Given the following report text (Hebrew, may include tables), extract the fields below.
If a field is not present, return null for that field.
Return ONLY valid JSON (no comments, no markdown fences).

Required JSON keys (extract these AND any others you find):
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

Example output (note the extra 'standby_hours' and 'sick_minutes' fields):
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
  "sick_days": null,
  "holiday_days": 0,
  "standby_hours": 10.5,
  "sick_minutes": 483
}}

Text to analyze:
---
{raw_text}
---
"""
    # --- END OF EDITED SECTION 3 ---

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Using a smaller model for cost/speed
            messages=[
                {"role": "system",
                 "content": "You extract precise structured data from Hebrew attendance reports. Output JSON only. You must be flexible and capture all available summary fields, while strictly following all rules about name correction and data validation."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        response_text = response.choices[0].message.content.strip()
        # Strip possible fences if any
        cleaned = response_text.replace("```json", "").replace("```", "").strip()
        data = json.loads(cleaned)

        # We no longer strictly enforce *only* the expected keys.
        # We just ensure the minimum keys are present (set to null if missing)
        expected = [
            "employee_name", "employee_id", "employee_number", "report_month",
            "total_presence_hours", "total_approved_hours", "total_payable_hours",
            "overtime_hours", "vacation_days", "sick_days", "holiday_days"
        ]
        for k in expected:
            data.setdefault(k, None)  # Add key if missing

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
def parse_report(file_path: str, email_metadata: dict | None = None) -> dict: # <-- חתימה מעודכנת
    """
    Detect file type, extract text (with OCR fallback for PDFs), send to LLM,
    and return a dict aligned with main.py printing logic.
    """
    
    # --- *** START: DEBUG TIMING *** ---
    t_start = time.time()
    # --- *** END: DEBUG TIMING *** ---

    p = Path(file_path)
    file_name = p.name

    result = {
        "file": file_name,
        "employee_name": None,
        "employee_id": None,
        "report_period": None,  # maintained for main.py compatibility
        "report_summary": None  # will hold a dict of all totals
    }

    try:
        # 1) Extract raw text
        suffix = p.suffix.lower().strip()
        supported_types = [".pdf", ".xlsx", ".xls", ".xlsm", ".csv", ".txt"]
        if suffix not in supported_types:
            print(f"--- Skipping unsupported file type: {file_name} ---")
            return result

        try:
            if suffix == ".pdf":
                raw_text = _extract_text_from_pdf(file_path)
                if _looks_unreadable(raw_text) or not raw_text.strip():
                    print(f"⚠️ {file_name}: PDF unreadable or empty — forcing OCR.")
                    raw_text = extract_text_with_ocr(file_path)
            elif suffix == ".txt":
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        raw_text = f.read()
                    print(f"ℹ️ {file_name}: Reading as plain text file.")
                except Exception as e:
                    print(f"⚠️ {file_name}: Error reading .txt file — {e}")
                    raw_text = ""
            else:
                raw_text = _extract_text_from_excel_or_csv(file_path)
        except Exception as e:
            print(f"⚠️ {file_name}: Error reading file — {e}")
            raw_text = ""

        # Safeguard: OCR fallback for empty text after reading
        if not raw_text.strip():
            print(f"⚠️ {file_name}: Empty text detected — forcing OCR as last resort.")
            raw_text = extract_text_with_ocr(file_path)

        if not (raw_text or "").strip():
            print(f"--- Skipping empty file: {file_name} ---")
            return result

        # --- *** START: DEBUG TIMING *** ---
        t_text_extracted = time.time()
        # --- *** END: DEBUG TIMING *** ---

        # Normalize terms for numeric extraction
        normalized_text = normalize_terms(raw_text)
        numeric_hints = _preextract_numeric_summary(normalized_text)

        # Convert values to floats
        for k, v in numeric_hints.items():
            if v is not None:
                numeric_hints[k] = float(v)

        # === חדש: חיפוש שם במטא-דאטה של המייל (עם Fuzzy) ===
        name_hint_from_email = None
        if email_metadata:
            name_hint_from_email = _find_name_from_email_metadata(email_metadata, EMPLOYEE_NAMES)
        # === סוף קטע חדש ===

        # === קיים: חיפוש שם בקובץ עצמו ===
        name_hint_from_regex = _find_name_with_regex(raw_text)

        # 2) LLM analysis → structured JSON
        llm_data = _analyze_text_with_llm(
            raw_text,
            file_name,
            hints=numeric_hints,
            name_hint_regex=name_hint_from_regex, # <-- רמז מהקובץ
            name_hint_email=name_hint_from_email  # <-- רמז מהמייל (עדיפות עליונה)
        )
        
        # --- *** START: DEBUG TIMING *** ---
        t_llm_done = time.time()
        # --- *** END: DEBUG TIMING *** ---

        if not llm_data:
            return result  # Failed LLM analysis

        # 3) Map to expected shape for main.py
        result["employee_name"] = llm_data.pop("employee_name", None)
        result["employee_id"] = llm_data.pop("employee_id", None) or llm_data.pop("employee_number", None)
        result["report_period"] = llm_data.pop("report_month", None)

        # --- Build the summary dict ---
        # (Keys that are always expected, even if null)
        summary_keys = [
            "total_presence_hours", "total_approved_hours", "total_payable_hours",
            "overtime_hours", "vacation_days", "sick_days", "holiday_days"
        ]
        report_summary = {}

        # Add the minimum required keys first
        for k in summary_keys:
            report_summary[k] = llm_data.pop(k, None)

        # Add any *other* keys the LLM found (for flexibility)
        # This loop takes remaining items from llm_data
        for k, v in llm_data.items():
            if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '', 1).isdigit()):
                report_summary[k] = v

        # --- Apply sanity checks ---
        report_summary = _apply_sanity_checks(report_summary)

        result["report_summary"] = report_summary

        # --- Override LLM hour summary with pre-extracted hints if they exist ---
        for key in result["report_summary"].keys():
            if key in numeric_hints and numeric_hints[key] is not None:
                if result["report_summary"][key] is None or result["report_summary"][key] != numeric_hints[key]:
                    if DEBUG: print(f"💡 Overriding '{key}' with pre-extracted hint: {numeric_hints[key]}")
                    result["report_summary"][key] = numeric_hints[key]
        
        # --- Apply sanity checks again after override ---
        result["report_summary"] = _apply_sanity_checks(result["report_summary"])

        # --- ייצוא CSV נפרד בוטל ---
        # The logic for exporting individual CSV/JSON files here has been removed.

        ALL_RESULTS.append(result)
        
        # --- *** START: DEBUG TIMING *** ---
        t_end = time.time()
        if DEBUG: # רק אם דיבאג דלוק
            print(f"  DEBUG: Text extraction took: {t_text_extracted - t_start:.2f}s")
            print(f"  DEBUG: LLM analysis took: {t_llm_done - t_text_extracted:.2f}s")
            print(f"  DEBUG: Post-processing took: {t_end - t_llm_done:.2f}s")
            print(f"  DEBUG: Total parse time: {t_end - t_start:.2f}s")
        # --- *** END: DEBUG TIMING *** ---

        return result
    except Exception as e:
        print(f"❌ Error parsing {file_name}: {e}")
        return result


# (No __main__ block – main.py runs this module.)

def export_all_results():
    """
    Export the entire ALL_RESULTS list into a single JSON and CSV file
    in the current working directory.
    (Note: This function is not called by main.py, which uses export_to_excel.py)
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
            if summary:  # Ensure summary is not None
                for k, v in summary.items():
                    row[k] = v
            csv_rows.append(row)
            all_fieldnames.update(row.keys())

        fieldnames = sorted([k for k in all_fieldnames if k != "report_summary"])

        with open(csv_path, "w", encoding="utf-8", newline='') as cf:
            writer = csv.DictWriter(cf, fieldnames=fieldnames)
            writer.writeheader()
            for row in csv_rows:
                row_to_write = {k: row.get(k) for k in fieldnames if k in row}
                writer.writerow(row_to_write)
        print(f"✅ Consolidated parsed results exported to CSV: {csv_path}")

    except Exception as e:
        print(f"⚠️ Failed to export consolidated parsed results: {e}")
        