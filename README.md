# Extreme AI Time Analyzer

## 1. Overview

This project provides an automated solution for processing employee time-sheets. It connects to a Gmail account, fetches unread timesheet reports from a specific label, parses them using AI, and organizes the structured data into a central Google Sheet.

The system is designed to handle multiple file types (PDF, XLSX, CSV) and uses OCR as a fallback for image-based or unreadable PDFs.

## 2. Core Features

* **Gmail Integration**: Automatically fetches email attachments (PDF, XLSX, CSV) from a specified Gmail label (`"Timesheet Reports"`).
* **AI-Powered Parsing**: Uses OpenAI (`gpt-4o-mini`) to intelligently analyze unstructured text from all file types and return structured JSON data.
* **OCR Fallback**: Includes an OCR engine (`pytesseract`) to handle scanned or unreadable PDFs, ensuring maximum data recovery.
* **Smart Deduplication**: Intelligently deduplicates entries based on a combination of employee name and ID. It automatically keeps the record with the *most complete data*.
* **Google Sheets Integration**: Automatically updates a central "Data Sheet" by cross-referencing an employee "Master Sheet". It creates new monthly tabs as needed and populates the data.
* **Local Backup**: Generates a local, deduplicated Excel summary (`all_reports_summary_YYYY-MM-DD.xlsx`) in the `downloads/reports/` directory upon every run.

## 3. How it Works (Workflow)

1.  `main.py` is executed.
2.  `gmail_fetcher.py` connects to Gmail, searches for the `"Timesheet Reports"` label, and downloads all attachments to the `downloads/` folder.
3.  `report_parser.py` iterates through each downloaded file:
    * It extracts text (using `pdfplumber`, `pandas`, or `ocr_extractor` as needed).
    * It sends the raw text to OpenAI, which returns structured JSON data.
    * All results are collected into the `ALL_RESULTS` list.
4.  `export_to_excel.py` deduplicates the list and saves the local `.xlsx` backup.
5.  `google_sheets_updater.py` performs its own deduplication, connects to Google Sheets, reads the Master Sheet, and updates the Data Sheet with the latest information.

## 4. Setup & Configuration

### Step 1: Install Dependencies
Install all required Python libraries from the `requirements.txt` file.
```bash
pip install -r requirements.txt
```

### Step 2: Google API Setup (One-Time)
This script uses a Google Service Account to edit the sheets.

1.  Go to the [Google Cloud Console](https://console.cloud.google.com/).
2.  Create a new project.
3.  In the "APIs & Services" library, enable both the **Google Sheets API** and the **Google Drive API**.
4.  Go to "Credentials" and create a **Service Account**.
5.  Grant the Service Account a role (e.g., "Editor").
6.  Create a **Key** for the service account, select **JSON**, and download the key file.
7.  Rename the downloaded file to `credentials.json` and place it in the root directory of this project.
8.  **Share your Google Sheets**: Open both the "Master Sheet" and the "Data Sheet" in Google Sheets, click "Share", and paste the email address of your new Service Account (e.g., `...gserviceaccount.com`). Give it **"Editor"** permissions.

### Step 3: Environment Variables
Create a file named `.env` in the root directory and add your credentials.
```
# From OpenAI Platform
OPENAI_API_KEY="sk-..."

# Gmail account that will be read
GMAIL_EMAIL="your-email@gmail.com"

# Gmail App Password (NOT your regular password)
GMAIL_APP_PASSWORD="your-app-password"

# Optional: Set to "true" for verbose logging
DEBUG="False"
```

### Step 4: Code Configuration
Some settings are hard-coded:

* **`google_sheets_updater.py`**:
    * `MASTER_SHEET_URL`: Must be set to the URL of your Master Employee Sheet.
    * `DATA_SHEET_URL`: Must be set to the URL of your output Data Sheet.
* **`gmail_fetcher.py`**:
    * `mail.select('"Timesheet Reports"')`: Ensure this label exists in your Gmail and contains the reports.
* **`terms_dictionary.json`**:
    * `employee_names`: Add known employee names to this list to improve the name-finding accuracy.

## 5. Usage

After all configuration is complete, simply run the main script:

```bash
python main.py
```