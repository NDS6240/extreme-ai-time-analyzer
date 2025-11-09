"""
ML Gatekeeper Model Training Script

This script performs two main actions, specified by a command-line argument:

Phase 1: 'collect'
- Scans the 'downloads' directory for reports (PDF, TXT, Excel).
- Extracts raw text from each file.
- Sends the text to OpenAI (LLM) for analysis to get a structured JSON.
- Creates a binary label:
    - 1 (Useful): If 'total_presence_hours' is found in the JSON.
    - 0 (Junk): If 'total_presence_hours' is None.
- Appends the (text, label, filename) to 'training_data.csv'.
- Deletes the processed file from 'downloads'.

Phase 2: 'train'
- Reads the 'training_data.csv'.
- Trains a Logistic Regression classifier pipeline (TfidfVectorizer + Model).
- Saves the final trained model pipeline to 'classifier_pipeline.pkl'.
"""

import os
import sys
import pandas as pd
import joblib
import argparse
import logging
from pathlib import Path

# --- Import necessary functions from the project ---
try:
    # Import text extraction and analysis functions from report_parser
    from report_parser import (
        _analyze_text_with_llm,
        _extract_text_from_pdf,
        _extract_text_from_excel_or_csv,
        _looks_unreadable
    )
    # Import the OCR fallback function
    from ocr_extractor import extract_text_with_ocr
except ImportError as e:
    print(f"Error: Failed to import modules. Make sure you are running from the project root.")
    print(f"Details: {e}")
    sys.exit(1)

# --- Import ML Libraries ---
try:
    from sklearn.model_selection import train_test_split
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.metrics import accuracy_score, classification_report
except ImportError:
    print("Error: scikit-learn or pandas not found.")
    print("Please update your requirements.txt and run: pip install -r requirements.txt")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Globals ---
DOWNLOADS_DIR = Path("downloads")
TRAINING_FILE = Path("training_data.csv")
MODEL_FILE = Path("classifier_pipeline.pkl")


def get_raw_text(file_path: Path) -> str:
    """
    Extracts raw text from a file, including OCR fallback logic.
    This duplicates the core text extraction logic from the main processing flow.
    """
    suffix = file_path.suffix.lower()
    raw_text = ""
    
    try:
        if suffix == ".pdf":
            raw_text = _extract_text_from_pdf(str(file_path))
            if _looks_unreadable(raw_text) or not raw_text.strip():
                logging.warning(f"PDF unreadable for {file_path.name}, forcing OCR.")
                raw_text = extract_text_with_ocr(str(file_path))
        elif suffix == ".txt":
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_text = f.read()
        elif suffix in [".xlsx", ".xls", ".csv"]:
            raw_text = _extract_text_from_excel_or_csv(str(file_path))
    except Exception as e:
        logging.error(f"Failed to extract text from {file_path.name}: {e}")
        return ""
        
    return raw_text.strip()


def collect_data():
    """
    Phase 1: Scans 'downloads', analyzes files via OpenAI,
    creates labels, and saves to a CSV.
    """
    logging.info("--- Starting Phase 1: Data Collection ---")
    
    if not DOWNLOADS_DIR.exists():
        logging.error(f"Downloads directory not found at: {DOWNLOADS_DIR}")
        return

    supported_extensions = [".pdf", ".xlsx", ".xls", ".csv", ".txt"]
    files_to_process = [
        f for f in DOWNLOADS_DIR.glob("*")
        if f.suffix.lower() in supported_extensions and "_parsed" not in f.name and not f.name.startswith(".")
    ]

    if not files_to_process:
        logging.warning("No files found in 'downloads' to process.")
        return

    logging.info(f"Found {len(files_to_process)} files to label.")
    
    training_data = []
    
    for i, file_path in enumerate(files_to_process, 1):
        logging.info(f"Processing file {i}/{len(files_to_process)}: {file_path.name}")
        
        # 1. Extract text
        raw_text = get_raw_text(file_path)
        if not raw_text:
            logging.warning(f"Skipping {file_path.name}: No text extracted.")
            continue
            
        # 2. Analyze with LLM (to determine the label)
        try:
            # We pass hints=None and name_hint=None as we only care about the output
            llm_data = _analyze_text_with_llm(raw_text, file_path.name, hints=None, name_hint=None)
        except Exception as e:
            logging.error(f"LLM analysis failed for {file_path.name}: {e}")
            continue

        # 3. Create label
        label = 0  # 0 = 'Junk' (default)
        if llm_data and llm_data.get('total_presence_hours') is not None:
            label = 1  # 1 = 'Useful'
            logging.info(f"  -> Labeled as USEFUL (1)")
        else:
            logging.info(f"  -> Labeled as JUNK (0)")
            
        training_data.append({"text": raw_text, "label": label, "filename": file_path.name})
        
        # 4. Delete the file after processing to avoid re-labeling
        try:
            os.remove(file_path)
            logging.info(f"  -> Removed processed file: {file_path.name}")
        except OSError as e:
            logging.warning(f"  -> Could not remove file: {e}")

    # 5. Save data
    if not training_data:
        logging.error("No data was collected.")
        return

    df = pd.DataFrame(training_data)
    
    # Check if data file already exists
    if TRAINING_FILE.exists():
        logging.info(f"Appending data to existing {TRAINING_FILE}")
        df.to_csv(TRAINING_FILE, mode='a', header=False, index=False, encoding='utf-8')
    else:
        logging.info(f"Creating new training file: {TRAINING_FILE}")
        df.to_csv(TRAINING_FILE, mode='w', header=True, index=False, encoding='utf-8')
        
    logging.info(f"--- Data Collection Complete. {len(df)} new records saved. ---")


def train_model():
    """
    Phase 2: Reads the labeled CSV, trains a classifier,
    and exports it to a .pkl file.
    """
    logging.info("--- Starting Phase 2: Model Training ---")
    
    if not TRAINING_FILE.exists():
        logging.error(f"Training file not found: {TRAINING_FILE}")
        logging.error("Please run the 'collect' action first.")
        return

    # 1. Load data
    df = pd.read_csv(TRAINING_FILE)
    
    # Basic cleaning
    df.dropna(subset=['label'], inplace=True) # Remove rows where labeling failed
    df['text'] = df['text'].fillna('') # Fill empty text with empty string
    
    if df.empty or len(df['label'].unique()) < 2:
        logging.error("Not enough data or only one class present. Need both 0s and 1s to train.")
        return

    logging.info(f"Loaded {len(df)} records from {TRAINING_FILE}")
    logging.info(f"Class distribution:\n{df['label'].value_counts(normalize=True)}")

    # 2. Define data and split
    X = df['text']
    y = df['label'].astype(int)
    
    # Split into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    # 3. Build the Pipeline
    # This bundles text vectorization and classification into one object.
    # We use simple, robust settings.
    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(
            max_features=3000,      # Limit features to top 3000 words/phrases
            ngram_range=(1, 2)     # Include single words and two-word phrases
        )),
        ('clf', LogisticRegression(
            random_state=42, 
            class_weight='balanced' # Helps model learn from unbalanced data
        ))
    ])

    # 4. Train
    logging.info("Training the model...")
    pipeline.fit(X_train, y_train)

    # 5. Evaluate
    logging.info("Evaluating model performance...")
    y_pred = pipeline.predict(X_test)
    
    accuracy = accuracy_score(y_test, y_pred)
    logging.info(f"\n--- Model Evaluation ---")
    logging.info(f"Accuracy: {accuracy:.4f}")
    logging.info("\nClassification Report:")
    # Print report to stdout for clarity
    print(classification_report(y_test, y_pred, target_names=['Junk (0)', 'Useful (1)']))
    logging.info("------------------------")

    # 6. Save the trained model pipeline
    joblib.dump(pipeline, MODEL_FILE)
    logging.info(f"Model pipeline saved successfully to: {MODEL_FILE}")
    logging.info("--- Model Training Complete ---")


if __name__ == "__main__":
    # --- Argument Parser ---
    parser = argparse.ArgumentParser(
        description="Gatekeeper ML Model Training Script",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "action", 
        choices=["collect", "train"], 
        help="Action to perform:\n"
             "  'collect' - Scans 'downloads', labels files using OpenAI, saves to CSV.\n"
             "  'train'   - Trains a model on the existing 'training_data.csv'."
    )
    
    args = parser.parse_args()
    
    # --- Execute Action ---
    if args.action == "collect":
        collect_data()
    elif args.action == "train":
        train_model()
