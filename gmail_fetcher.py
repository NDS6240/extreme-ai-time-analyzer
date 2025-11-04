import os
import imaplib
import email
from email.header import decode_header
from pathlib import Path
from dotenv import load_dotenv
import re  # *** התיקון כאן - הוספת הייבוא החסר ***
from datetime import datetime, timedelta

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

        # Calculate date range (current month) for IMAP search filtering
        today = datetime.now()
        first_day_current_month = today.replace(day=1)
        first_day_next_month = (first_day_current_month + timedelta(days=32)).replace(day=1)

        search_start_str = first_day_current_month.strftime("%d-%b-%Y")
        search_end_str = first_day_next_month.strftime("%d-%b-%Y")
        search_query = f'(SINCE "{search_start_str}" BEFORE "{search_end_str}")'

        mail.login(EMAIL_ACCOUNT, APP_PASSWORD)
        # Select the Gmail label instead of inbox
        mail.select('"Timesheet Reports"')
        print(f"🔍 Searching for messages from {search_start_str} to before {search_end_str}...")

        # ---
        # שונה מ-'NOT DELETED' ל-'ALL' כדי לכלול את כל המיילים בתווית
        status, messages = mail.search(None, search_query)
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

                body_text_part = None

                for part in msg.walk():
                    if part.get_content_maintype() == "multipart":
                        continue

                    disposition = part.get('Content-Disposition', '') or ''
                    is_attachment = disposition.startswith(('attachment', 'inline'))

                    if is_attachment:
                        filename = part.get_filename()
                        if not filename:
                            continue

                        decoded_filename, enc = decode_header(filename)[0]
                        if isinstance(decoded_filename, bytes):
                            decoded_filename = decoded_filename.decode(enc or "utf-8", errors="ignore")

                        decoded_filename = re.sub(r'[\\/*?:"<>|]', "", decoded_filename)

                        if not decoded_filename:
                            print("⚠️ Found attachment with invalid/empty filename, skipping.")
                            continue

                        if decoded_filename.lower().endswith((".pdf", ".xlsx", ".csv", ".xls")):
                            filepath = DOWNLOAD_DIR / decoded_filename

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

                    elif part.get_content_type() == "text/plain":
                        if body_text_part:
                            continue

                        body_payload = part.get_payload(decode=True)
                        if not body_payload:
                            continue

                        body_text = body_payload.decode(part.get_content_charset() or "utf-8", errors="ignore")

                        if body_text and body_text.strip():
                            sanitized_subject = re.sub(r'[\\/*?:"<>|]', "", subject)
                            if not sanitized_subject:
                                sanitized_subject = "EmailBody"
                            msg_id_str = num.decode() if isinstance(num, bytes) else str(num)

                            filename = f"{sanitized_subject}_Body_{msg_id_str}.txt"
                            filepath = DOWNLOAD_DIR / filename

                            counter = 1
                            original_filepath = filepath
                            while filepath.exists():
                                filepath = DOWNLOAD_DIR / f"{original_filepath.stem}_{counter}{original_filepath.suffix}"
                                counter += 1

                            try:
                                with open(filepath, "w", encoding="utf-8") as f:
                                    f.write(body_text)
                                print(f"✅ Saved email body: {filepath.name}")
                                body_text_part = body_text
                            except IOError as e:
                                print(f"❌ Error writing email body file {filepath.name}: {e}")

            except Exception as e:
                msg_id_str = num.decode() if isinstance(num, bytes) else str(num)
                print(f"❌ Error processing message {msg_id_str}: {e}")

        mail.logout()
        print("📥 All attachments downloaded successfully!")

    except Exception as e:
        print("❌ Error fetching reports:", e)


if __name__ == "__main__":
    fetch_reports_from_gmail()
