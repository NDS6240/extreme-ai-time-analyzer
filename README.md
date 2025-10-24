# Extreme AI Time Analyzer

Automated AI-based timesheet extraction and validation system for Extreme HR.

## Overview
This project automatically:
1. Fetches reports from Gmail (PDFs, Excel, Scanned documents)
2. Extracts employee names, IDs, months, and working hours
3. Learns document layouts automatically using `zones_templates`
4. Uses OCR and AI to parse even complex scanned reports
5. Exports consolidated Excel summaries

## Main Components
| File | Description |
|------|--------------|
| `main.py` | Entry point. Fetches new reports and triggers parsing. |
| `report_parser.py` | Core logic: text extraction, normalization, and AI analysis. |
| `template_trainer.py` | Detects new layouts and generates zone templates. |
| `dynamic_extractor.py` | AI-based text summarizer for numeric and semantic data. |
| `ocr_extractor.py` | Fallback OCR extraction for scanned reports. |

## Pipeline
1. **Gmail Fetcher** → Downloads all reports.
2. **Report Parser** → Analyzes structure (zone-based + dynamic).
3. **Extractor** → Uses regex + LLM to extract key values.
4. **Excel Exporter** → Aggregates all reports into one summary file.

## Caching
All parsed files are cached as `.parsed.json` to prevent redundant processing.

## Output
All extracted data is consolidated in: