import pdfplumber
import json
import os


def extract_word_positions(pdf_path):
    """
    Extracts and prints all words and their bounding boxes in the PDF.
    """
    with pdfplumber.open(pdf_path) as pdf:
        for page_number, page in enumerate(pdf.pages):
            print(f"\n--- Page {page_number + 1} ---")
            words = page.extract_words()
            for word in words:
                text = word.get('text')
                x0, top, x1, bottom = word['x0'], word['top'], word['x1'], word['bottom']
                print(f"Word: '{text}' | x0={x0:.1f}, top={top:.1f}, x1={x1:.1f}, bottom={bottom:.1f}")
    return


def auto_detect_zones(pdf_path):
    """
    Heuristically detects key zones in the PDF using keywords and numeric clusters.
    Returns a dict with zone names and bounding boxes (per page).
    """
    zones = {}
    with pdfplumber.open(pdf_path) as pdf:
        for page_number, page in enumerate(pdf.pages):
            words = page.extract_words()
            # Detect employee name zone (Hebrew: שם העובד)
            name_zone = None
            for word in words:
                if "שם העובד" in word['text']:
                    # Expand a rectangle around the keyword
                    expand = 70  # pixels to the right
                    name_zone = {
                        "x0": word['x0'],
                        "top": word['top'],
                        "x1": word['x1'] + expand,
                        "bottom": word['bottom'] + 10
                    }
                    break
            # Detect hours table zone: find a block of numeric words
            numeric_words = [w for w in words if w['text'].replace('.', '', 1).isdigit()]
            if numeric_words:
                # Cluster numerics by vertical proximity
                tops = [w['top'] for w in numeric_words]
                bottoms = [w['bottom'] for w in numeric_words]
                x0s = [w['x0'] for w in numeric_words]
                x1s = [w['x1'] for w in numeric_words]
                # Heuristic: take min/max of cluster
                hours_zone = {
                    "x0": min(x0s) - 5,
                    "top": min(tops) - 5,
                    "x1": max(x1s) + 5,
                    "bottom": max(bottoms) + 5,
                }
            else:
                hours_zone = None
            zones[f"page_{page_number + 1}"] = {
                "employee_name_zone": name_zone,
                "hours_table_zone": hours_zone
            }
    print("Auto-detected zones:")
    for page, zone_dict in zones.items():
        print(f"{page}:")
        for zone_name, bbox in zone_dict.items():
            print(f"  {zone_name}: {bbox}")
    return zones


def create_zone_template(pdf_path, output_json):
    """
    Interactively or automatically creates a zones template and saves it to JSON.
    """
    print(f"Attempting to auto-detect zones in {pdf_path}...")
    zones = auto_detect_zones(pdf_path)
    print("\nReview the detected zones above.")
    # Optionally, user could edit here. For now, just save.
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(zones, f, indent=2, ensure_ascii=False)
    print(f"Zones template saved to {output_json}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PDF Template Trainer")
    parser.add_argument("pdf", help="Path to the highlighted PDF file")
    parser.add_argument("--extract-words", action="store_true", help="Print word positions for inspection")
    parser.add_argument("--make-template", action="store_true", help="Create a zone template and save to JSON")
    parser.add_argument("--output", default="zones_template.json", help="Output JSON file for template")
    args = parser.parse_args()

    if not os.path.isfile(args.pdf):
        print(f"File not found: {args.pdf}")
        exit(1)

    if args.extract_words:
        extract_word_positions(args.pdf)
    if args.make_template:
        create_zone_template(args.pdf, args.output)
    if not args.extract_words and not args.make_template:
        print("No action specified. Use --extract-words or --make-template.")
