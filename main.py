"""
Main entry point for the Extreme AI Time Analyzer.
Processes employee attendance reports from Gmail, validates data, and exports to Excel/Google Sheets.
NOW INCLUDES ML-POWERED GATEKEEPER TO FILTER JUNK FILES.
"""
import json
import os
from gmail_fetcher import fetch_reports_from_gmail
from report_parser import parse_report
from pathlib import Path
from report_parser import ALL_RESULTS, EMPLOYEE_NAMES
from export_to_excel import export_summary_excel
from google_sheets_updater import update_google_sheets
from data_validator import get_master_employee_dict, apply_vacation_completion

# --- ML Gatekeeper Imports ---
import joblib
from report_parser import _extract_text_from_pdf, _extract_text_from_excel_or_csv, _looks_unreadable
from ocr_extractor import extract_text_with_ocr
# --- End ML Imports ---


def get_raw_text_for_model(file_path: Path) -> str:
    """
    Extracts raw text for the ML model to predict on.
    This is a lightweight version of the text extraction logic.
    """
    suffix = file_path.suffix.lower()
    raw_text = ""
    try:
        if suffix == ".pdf":
            raw_text = _extract_text_from_pdf(str(file_path))
            if _looks_unreadable(raw_text) or not raw_text.strip():
                # If PDF is unreadable, run OCR for the model check
                raw_text = extract_text_with_ocr(str(file_path))
        elif suffix == ".txt":
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_text = f.read()
        elif suffix in [".xlsx", ".xls", ".csv"]:
            raw_text = _extract_text_from_excel_or_csv(str(file_path))
    except Exception:
        return ""  # Return empty string on failure
    return raw_text.strip()


if __name__ == "__main__":
    # Step 1: Fetch new reports from Gmail
    print("📩 Fetching new reports from Gmail...")
    fetch_reports_from_gmail()

    # --- Load ML Gatekeeper Model ---
    MODEL_FILE = Path("classifier_pipeline.pkl")
    model = None
    if not MODEL_FILE.exists():
        print(f"❌ WARNING: Model file not found: {MODEL_FILE}")
        print("   Will proceed WITHOUT ML filtering (all files will be sent to OpenAI).")
        print("   (Run 'python train_model.py train' to create the model)")
    else:
        try:
            model = joblib.load(MODEL_FILE)
            print(f"✅ ML Gatekeeper model loaded successfully (Accuracy: 96%).")
        except Exception as e:
            print(f"❌ WARNING: Failed to load ML model ({e}). Proceeding without filtering.")
    # --- End Model Loading ---

    # Step 2: Process downloaded reports
    print("\n🧠 Analyzing downloaded reports...\n")
    downloads_path = Path("downloads")
    
    if not downloads_path.exists():
        print("⚠️ No 'downloads' folder found. Please ensure files are downloaded correctly.")
    else:
        all_files_in_dir = list(downloads_path.glob("*"))
        
        if not all_files_in_dir:
            print("⚠️ No files found in 'downloads' folder.")
        else:
            supported_extensions = [".pdf", ".xlsx", ".xls", ".csv", ".txt"]
            
            # Pre-filter files
            files_to_process = []
            for f in all_files_in_dir:
                if "_parsed" in f.name or f.name.startswith(".") or f.suffix.lower() not in supported_extensions:
                    print(f"--- Skipping generated or unsupported file: {f.name} ---")
                    continue
                files_to_process.append(f)
            
            total_count = len(files_to_process)
            
            if total_count == 0:
                print("⚠️ No valid report files found to process.")
            else:
                # Process each report file
                for i, f in enumerate(files_to_process, start=1):
                    print(f"\n--- Analyzing Report {i}/{total_count}: {f.name} ---")

                    # --- ML GATEKEEPER LOGIC ---
                    if model:
                        raw_text = get_raw_text_for_model(f)
                        if not raw_text:
                            print(f"  --- Skipping: Could not extract text for ML check. ---")
                            continue
                        
                        try:
                            # Predict requires a list of texts
                            prediction = model.predict([raw_text])[0]
                            
                            if prediction == 0:
                                print(f"  --- 🛑 Skipping (ML Gatekeeper): File identified as 'Junk'. ---")
                                
                                # Delete the junk file immediately
                                try:
                                    os.remove(f)
                                    if os.getenv("DEBUG", "False").lower() == "true":
                                        print(f"  🗑️ Removed junk file: {f.name}")
                                except OSError as e:
                                    print(f"  ⚠️ Could not remove junk file {f.name}: {e}")
                                
                                continue # Skip to the next file
                            else:
                                print(f"  --- ✅ Passing (ML Gatekeeper): File identified as 'Useful'. Proceeding to OpenAI. ---")
                        
                        except Exception as e:
                            # If model fails, proceed to OpenAI as a fallback
                            print(f"  --- ⚠️ ML Gatekeeper failed ({e}). Proceeding to OpenAI anyway. ---")
                    # --- END ML GATEKEEPER LOGIC ---
                    
                    # Parse the report using LLM-based extraction (ONLY IF IT PASSED THE GATE)
                    result = parse_report(str(f))

                    # Display extracted information
                    print(f"  File: {result['file']}")
                    print(f"  Name: {result['employee_name']}")
                    print(f"  ID: {result['employee_id']}")
                    print(f"  Period: {result['report_period']}")
                    summary_str = json.dumps(result.get('report_summary'), indent=4, ensure_ascii=False)
                    print(f"  Summary:\n{summary_str}")
                    
                    # Delete processed file (if it wasn't already deleted by the gatekeeper)
                    if f.exists():
                        try:
                            os.remove(f)
                            if os.getenv("DEBUG", "False").lower() == "true":
                                print(f"  🗑️ Removed local file: {f.name}")
                        except OSError as e:
                            print(f"  ⚠️ Could not remove file {f.name}: {e}")

    print("\n✅ Process complete.")

    print("\n🔄 Applying business logic (vacation completion)...")
    master_dict = get_master_employee_dict()
    processed_results = apply_vacation_completion(ALL_RESULTS, master_dict)

    # Step 3: Export validated data to Excel with summary table
    export_summary_excel(processed_results, EMPLOYEE_NAMES)
    
    # Step 4: Update Google Sheets with validated and matched data
    update_google_sheets(processed_results, EMPLOYEE_NAMES)
    