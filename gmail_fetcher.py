# gmail_fetcher.py
import os
import imaplib
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

EMAIL_ACCOUNT = os.getenv("GMAIL_EMAIL")
APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")


def test_gmail_connection():
    """Tests basic connection to the Gmail account"""
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_ACCOUNT, APP_PASSWORD)
        print("✅ Successfully connected to account:", EMAIL_ACCOUNT)
        mail.logout()
    except Exception as e:
        print("❌ Connection error:", e)


if __name__ == "__main__":
    test_gmail_connection()
# gmail_fetcher.py
import os
import imaplib
import email
from email.header import decode_header
from pathlib import Path
from dotenv import load_dotenv

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

        # Search all messages in that label
        status, messages = mail.search(None, 'ALL')
        if status != "OK":
            print("❌ No messages found.")
            return

        for num in messages[0].split():
            _, msg_data = mail.fetch(num, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            subject, encoding = decode_header(msg["Subject"])[0]
            if isinstance(subject, bytes):
                subject = subject.decode(encoding or "utf-8", errors="ignore")

            print(f"📩 Processing email: {subject}")

            for part in msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue

                filename = part.get_filename()
                if filename:
                    decoded_filename, enc = decode_header(filename)[0]
                    if isinstance(decoded_filename, bytes):
                        decoded_filename = decoded_filename.decode(enc or "utf-8", errors="ignore")

                    if decoded_filename.lower().endswith((".pdf", ".xlsx", ".csv")):
                        filepath = DOWNLOAD_DIR / decoded_filename
                        with open(filepath, "wb") as f:
                            f.write(part.get_payload(decode=True))
                        print(f"✅ Saved attachment: {filepath}")

        mail.logout()
        print("📥 All attachments downloaded successfully!")

    except Exception as e:
        print("❌ Error fetching reports:", e)


if __name__ == "__main__":
    fetch_reports_from_gmail()
