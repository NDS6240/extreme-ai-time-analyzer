import os
import imaplib
import email
from email.header import decode_header
from pathlib import Path
from dotenv import load_dotenv
import re  # *** התיקון כאן - הוספת הייבוא החסר ***

# Load environment variables from .env file
load_dotenv()

EMAIL_ACCOUNT = os.getenv("GMAIL_EMAIL")
APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)


def fetch_reports_from_gmail():
    """Fetches attachments (PDF, XLSX, CSV) from Gmail inbox"""
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_ACCOUNT, APP_PASSWORD)
        # Select the Gmail label instead of inbox
        mail.select('"Timesheet Reports"')

        # ---
        # שונה מ-'NOT DELETED' ל-'ALL' כדי לכלול את כל המיילים בתווית
        status, messages = mail.search(None, '(ALL)')
        # ---

        if status != "OK":
            print("❌ No messages found.")
            return

        message_ids = messages[0].split()
        print(f"🔍 Found {len(message_ids)} messages in label.")

        for num in message_ids:
            # הוספת בדיקה למקרה שמספר ההודעה ריק
            if not num:
                continue

            try:
                _, msg_data = mail.fetch(num, "(RFC822)")
                # הוספת בדיקה למקרה ש-msg_data ריק או בפורמט לא צפוי
                if not msg_data or not msg_data[0] or not isinstance(msg_data[0], (tuple, list)) or len(
                        msg_data[0]) < 2:
                    print(f"⚠️ Skipping message {num}: Invalid data structure.")
                    continue

                raw_email = msg_data[0][1]
                if not raw_email:
                    print(f"⚠️ Skipping message {num}: Empty email body.")
                    continue

                msg = email.message_from_bytes(raw_email)

                subject = "No Subject"
                if msg["Subject"]:
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding or "utf-8", errors="ignore")

                # המרת ID ל-string לצורך הדפסה בטוחה
                msg_id_str = num.decode() if isinstance(num, bytes) else str(num)
                print(f"📩 Processing email: {subject} (ID: {msg_id_str})")

                for part in msg.walk():
                    if part.get_content_maintype() == "multipart" or part.get('Content-Disposition') is None:
                        continue

                    filename = part.get_filename()
                    if filename:
                        decoded_filename, enc = decode_header(filename)[0]
                        if isinstance(decoded_filename, bytes):
                            decoded_filename = decoded_filename.decode(enc or "utf-8", errors="ignore")

                        # ניקוי שם קובץ מתווים בעייתיים
                        decoded_filename = re.sub(r'[\\/*?:"<>|]', "", decoded_filename)

                        if not decoded_filename:
                            print("⚠️ Found attachment with invalid/empty filename, skipping.")
                            continue

                        if decoded_filename.lower().endswith((".pdf", ".xlsx", ".csv", ".xls")):
                            filepath = DOWNLOAD_DIR / decoded_filename

                            # מניעת דריסה של קבצים באותו שם (מוסיף מספר)
                            counter = 1
                            original_filepath = filepath
                            while filepath.exists():
                                filepath = DOWNLOAD_DIR / f"{original_filepath.stem}_{counter}{original_filepath.suffix}"
                                counter += 1

                            try:
                                with open(filepath, "wb") as f:
                                    f.write(part.get_payload(decode=True))
                                print(f"✅ Saved attachment: {filepath.name}")
                            except IOError as e:
                                print(f"❌ Error writing file {filepath.name}: {e}")

            except Exception as e:
                msg_id_str = num.decode() if isinstance(num, bytes) else str(num)
                print(f"❌ Error processing message {msg_id_str}: {e}")

        mail.logout()
        print("📥 All attachments downloaded successfully!")

    except Exception as e:
        print("❌ Error fetching reports:", e)


if __name__ == "__main__":
    fetch_reports_from_gmail()
