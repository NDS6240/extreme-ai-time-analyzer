import os
import json
import pdfplumber
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# --- Configuration and API Key Loading ---
load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("Error: 'OPENAI_API_KEY' not found in .env file.")

client = OpenAI(api_key=API_KEY)


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


# --- OpenAI-based LLM analysis ---
def _analyze_text_with_llm(raw_text: str, file_name: str) -> dict | None:
    """
    Sends extracted text to OpenAI (GPT-4o-mini) to extract structured attendance data.
    Searches for Hebrew keywords related to employee info and summary totals.
    """
    if not raw_text.strip():
        print(f"⚠️ Skipping {file_name}, no text was extracted.")
        return None

    try:
        prompt = f"""
        You are an AI assistant that extracts structured data from Hebrew attendance reports.

        The following text is from a file called "{file_name}".
        Extract all relevant employee and summary fields, focusing on:

        Hebrew keywords like:
        "שם", "עובד", "ת.ז", "מספר עובד", "חודש", "סה\"כ", "נוכחות", "מאושרות",
        "לתשלום", "שעות נוספות", "חופשה", "מחלה", "חג", "סך".

        Return a JSON with the following structure:
        {{
          "employee_name": "",
          "employee_id": "",
          "employee_number": "",
          "report_month": "",
          "total_presence_hours": "",
          "total_approved_hours": "",
          "total_payable_hours": "",
          "overtime_hours": "",
          "vacation_days": "",
          "sick_days": "",
          "holiday_days": ""
        }}

        Do not include signatures, approvals, or manager info.
        Return **only valid JSON**, with null values for missing fields.

        Text to analyze:
        ---
        {raw_text}
        ---
        """

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise HR data extraction assistant."},
                {"role": "user", "content": prompt}
            ]
        )

        response_text = response.choices[0].message.content.strip()
        cleaned = response_text.replace("```json", "").replace("```", "").strip()
        data = json.loads(cleaned)
        return data

    except json.JSONDecodeError:
        print(f"❌ JSON Error: Model did not return valid JSON for {file_name}. Response: {response_text}")
    except Exception as e:
        print(f"❌ API Error for {file_name}: {e}")

    return None


# --- Main Public Function ---

def parse_report(file_path: str) -> dict:
    """
    Detects file type, extracts raw text, and sends it to OpenAI for structured analysis.
    Works with Hebrew text-based reports (PDF, Excel, CSV).
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

    if p.suffix.lower() == ".pdf":
        raw_text = _extract_text_from_pdf(file_path)
    elif p.suffix.lower() in [".xlsx", ".xls", ".csv"]:
        raw_text = _extract_text_from_excel_or_csv(file_path)
    else:
        print(f"--- Skipping unsupported file type: {file_name} ---")
        return result

    llm_result = _analyze_text_with_llm(raw_text, file_name)
    if llm_result:
        result.update(llm_result)

    return result


if __name__ == "__main__":
    print("--- Testing OpenAI-based Report Parser (v7) ---")

    downloads_path = Path("downloads")
    if not downloads_path.exists():
        print(f"⚠️ '{downloads_path}' directory not found.")
    else:
        supported_extensions = [".pdf", ".xlsx", ".xls", ".csv"]
        test_files = [f for ext in supported_extensions for f in downloads_path.glob(f"*{ext}")]
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