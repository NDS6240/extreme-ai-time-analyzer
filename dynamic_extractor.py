import re, json
from pathlib import Path


def time_to_decimal(t: str) -> float | None:
    if not t or not isinstance(t, str):
        return None
    t = t.strip()
    m = re.fullmatch(r"(\d+):(\d{1,2})", t)
    if m:
        h, m_ = int(m.group(1)), int(m.group(2))
        return round(h + m_ / 60, 2)
    try:
        return float(t)
    except:
        return None


def _preextract_dynamic_summary(text: str) -> dict:
    """
    Extract ALL numeric values from text near known work-related Hebrew terms.
    Returns dict { term: numeric_value }
    """
    result = {}
    dict_path = Path("terms_dictionary.json")
    if not dict_path.exists():
        print("⚠️ Missing terms_dictionary.json – dynamic extraction skipped.")
        return result

    try:
        terms_data = json.loads(dict_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"⚠️ Failed to read terms_dictionary.json: {e}")
        return result

    # Flatten all terms into a single list
    all_terms = []
    for group_terms in terms_data.values():
        all_terms.extend(group_terms)

    # Clean text for scanning
    text = text.replace("\n", " ")
    pattern_num = r"(\d+(?::\d{1,2})?|\d+\.\d+)"

    for term in all_terms:
        # Find up to 20 chars after the term, look for number
        regex = re.compile(re.escape(term) + r"[^0-9]{0,10}" + pattern_num)
        matches = regex.findall(text)
        if matches:
            # Take the highest plausible value
            decimals = [time_to_decimal(m if isinstance(m, str) else m[-1]) for m in matches]
            decimals = [d for d in decimals if d is not None]
            if decimals:
                result[term] = max(decimals)

    # Secondary pass: detect standalone triples of numbers (e.g., 160.00 159.25 159.24)
    triples = re.findall(r"(?<!\d)(\d+(?:\.\d+)?)[\s,]+(\d+(?:\.\d+)?)[\s,]+(\d+(?:\.\d+)?)(?!\d)", text)
    for a, b, c in triples:
        vals = [float(a), float(b), float(c)]
        result.setdefault("סיכום כולל שעות", max(vals))

    return result
