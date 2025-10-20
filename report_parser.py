import os
import json
import pdfplumber  # We are back to using pdfplumber
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import google.generativeai as genai

# --- Configuration and API Key Loading ---
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    raise ValueError("Error: 'GEMINI_API_KEY' not found in .env file.")
genai.configure(api_key=API_KEY)


# --- Helper function to extract text from digital PDF ---
def _extract_text_from_pdf(file_path: str) -> str:
    """Extracts all text from all pages of a PDF into a single string."""
    text = ""
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text += page.extract_text(x_tolerance=1, y_tolerance=1) or ""
        return text
    except Exception as e:
        print(f"⚠️ Error reading PDF {file_path}: {e}")
        return ""


# --- Helper function to extract text from Excel/CSV ---
def _extract_text_from_excel_or_csv(file_path: str) -> str:
    """Extracts all text from all sheets in an Excel/CSV file into a single string."""
    text = ""
    p = Path(file_path)
    try:
        if p.suffix.lower() in [".xlsx", ".xls"]:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                text += f"--- Sheet: {sheet_name} ---\n"
                text += df.to_string(index=False, header=False, na_rep='') + "\n\n"
        elif p.suffix.lower() == ".csv":
            df = pd.read_csv(file_path, header=None)
            text += "--- CSV Content ---\n"
            text += df.to_string(index=False, header=False, na_rep='') + "\n\n"
        return text.strip()
    except Exception as e:
        print(f"⚠️ Error reading Excel/CSV {file_path}: {e}")
        return ""


def _analyze_text_with_llm(raw_text: str, file_name: str) -> dict | None:
    """
    Sends raw text to the Gemini API and requests a detailed JSON structure.
    Uses the basic 'gemini-pro' model which is text-only.
    """
    if not raw_text.strip():
        print(f"⚠️ Skipping {file_name}, no text was extracted.")
        return None

    # --- MODEL CHANGE ---
    # Using 'gemini-pro' (the basic text-only model)
    # This should be available on your account after enabling the API.
    model = genai.GenerativeModel('gemini-pro')

    prompt = f"""
    You are an expert HR data extraction bot.
    Analyze the following raw text extracted from a timesheet report file named '{file_name}'.
    The text may be messy and unstructured.

    Your task is to analyze this text and extract the following information:
    1.  The full employee name (key: "employee_name").
    2.  The employee ID number (key: "employee_id").
    3.  The report period (key: "report_period"), e.g., "09/2025".
    4.  A dictionary of *all relevant summary totals* (key: "report_summary").
        - Find all relevant totals (e.g., "Total Presence", "Total Approved", "Total Standard", "Total Overtime").

    Return *only* a valid JSON object with these keys.
    If a value is not found, set it to null.

    Example:
    {{
      "employee_name": "Israel Israeli",
      "employee_id": "123456",
      "report_period": "09/2025",
      "report_summary": {{
        "Total Hours": "180:00",
        "Approved Hours": "175:30"
      }}
    }}

    Here is the text to analyze:
    ---
    {raw_text}
    ---
    """

    try:
        # Call the API with text-only content
        response = model.generate_content(
            prompt,
            generation_config=genai.types.GenerationConfig(
                response_mime_type="application/json"
            )
        )

        response_text = response.text.strip().replace("```json", "").replace("```", "").strip()
        data = json.loads(response_text)
        return data

    except json.JSONDecodeError as e:
        print(f"❌ JSON Error: Model did not return valid JSON for {file_name}. Response: {response.text}")
    except Exception as e:
        print(f"❌ API Error for {file_name}: {e}")

    return None


# --- Main Public Function ---

def parse_report(file_path: str) -> dict:
    """
    Detects file type, extracts raw text, and sends it to an LLM for analysis.
    This version does NOT support scanned/OCR files.
    """
    p = Path(file_path)
    file_name = p.name
    raw_text = ""
    result = {
        "file": file_name,
        "employee_name": None,
        "employee_id": None,
        "report_period": None,
        "report_summary": None
    }

    # Step 1: Extract raw text based on file extension
    if p.suffix.lower() == ".pdf":
        raw_text = _extract_text_from_pdf(file_path)
    elif p.suffix.lower() in [".xlsx", ".xls", ".csv"]:
        raw_text = _extract_text_from_excel_or_csv(file_path)
    else:
        # This will skip image files and unsupported types
        print(f"--- Skipping unsupported file type: {file_name} ---")
        return result

    # Step 2: Send the extracted text to the LLM for analysis
    llm_result = _analyze_text_with_llm(raw_text, file_name)

    # Step 3: Populate the result dictionary
    if llm_result:
        result.update(llm_result)

    return result


if __name__ == "__main__":
    # --- Local Test Runner ---
    print("--- Testing LLM-based Report Parser (v6 - Text Only) ---")

    downloads_path = Path("downloads")
    if not downloads_path.exists():
        print(f"⚠️ '{downloads_path}' directory not found.")
    else:
        # Back to only supporting text-based files
        supported_extensions = [".pdf", ".xlsx", ".xls", ".csv"]
        test_files = []
        for ext in supported_extensions:
            test_files.extend(list(downloads_path.glob(f"*{ext}")))

        if not test_files:
            print(f"⚠️ No supported text files found in '{downloads_path}' to test.")
        else:
            print(f"Found {len(test_files)} files to test...")
            for f in test_files:
                if f.is_file():
                    print(f"\n🧠 Analyzing: {f.name}...")
                    result = parse_report(str(f))
                    print(f"📄 Result: {json.dumps(result, indent=2, ensure_ascii=False)}")

    print("\n✅ Test complete.")
