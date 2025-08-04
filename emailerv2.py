import os
import csv
import ssl
import logging
import smtplib
from datetime import datetime
from time import sleep
from smtplib import SMTPResponseException
from email.message import EmailMessage
from dotenv import load_dotenv


# Load environment variables from .env
load_dotenv()

# ——— CONFIG ———
# Path to your HTML template:
HTML_PATH   = os.getenv("HTML_PATH", r"C:/Users/monke/Documents/gcwebapp/body.html")
SMTP_HOST   = "smtp.gmail.com"
SMTP_PORT   = 465
EMAIL_FROM  = os.getenv("SMTP_USER")
EMAIL_PASS  = os.getenv("SMTP_PASS")
CSV_PATH    = r"C:/Users/monke/Documents/gcwebapp/your_list.csv"
SUBJECT     = f"Upcoming events across MENA — {datetime.today():%Y-%m-%d}"

# ——— LOGGING ———
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


def build_message(to_addr: str, first_name: str) -> EmailMessage:
    """
    Build an EmailMessage where HTML_PATH is loaded as the main body
    and a plain-text version is added as an alternative.
    """
    name = first_name.strip() or "Sir/Madam"
    # Load and personalize HTML template
    try:
        with open(HTML_PATH, "r", encoding="utf-8") as f:
            template = f.read()
    except Exception as e:
        logging.error(f"Could not read HTML template at {HTML_PATH}: {e}")
        template = ""
    html = template.replace("{{FirstName}}", name)

    # Create email
    msg = EmailMessage()
    msg["From"]    = EMAIL_FROM
    msg["To"]      = to_addr
    msg["Subject"] = SUBJECT

    # HTML as primary content
    msg.set_content(html, subtype='html')

    # Plain-text fallback
    text = (
        f"Dear {name},\n\n"
        "I hope this email finds you well...\n\n"
        "Book a Meeting: https://meetings.hubspot.com/ak52\n\n"
        "Kind regards,\n"
        "Mohammed Osman"
    )
   
    return msg


import csv
from typing import List

def load_messages(path: str) -> List[str]:
    """
    Load recipients from CSV (headers: FirstName, Email) and build messages.
    Tries UTF-8 first, falls back to Windows‑1252 if decoding fails.
    """
    # Probe the file for UTF-8 validity
    try:
        with open(path, newline="", encoding="utf-8") as fp_test:
            fp_test.readline()  # trigger UnicodeDecodeError if not UTF-8
    except UnicodeDecodeError:
        encoding = "cp1252"
    else:
        encoding = "utf-8"

    # Now open for real with the detected encoding
    messages: List[str] = []
    with open(path, newline="", encoding=encoding) as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            first = row.get("FirstName", "").strip()
            to    = row.get("Email",     "").strip()
            if to:
                messages.append(build_message(to, first))

    return messages



def send_all(server: smtplib.SMTP_SSL, messages, base_delay=0.1, max_delay=10):
    """
    Send messages with adaptive back-off for transient SMTP errors.
    """
    delay = base_delay
    for msg in messages:
        try:
            server.send_message(msg)
            logging.info("Sent to %s", msg["To"])
            delay = max(base_delay, delay * 0.9)
        except SMTPResponseException as e:
            code = e.smtp_code
            if code in (421, 450, 451, 452):
                logging.warning("Throttled (%d), backing off %.1fs", code, delay)
                sleep(delay)
                delay = min(max_delay, delay * 2)
                try:
                    server.send_message(msg)
                    logging.info("Retry succeeded for %s", msg["To"])
                except Exception as e2:
                    logging.error("Retry failed for %s: %s", msg["To"], e2)
            else:
                logging.error("Failed to send to %s: %s", msg["To"], e)
        sleep(delay)


def main():
    if not EMAIL_FROM or not EMAIL_PASS:
        logging.critical("Environment variables SMTP_USER or SMTP_PASS not set.")
        return

    messages = load_messages(CSV_PATH)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
        server.login(EMAIL_FROM, EMAIL_PASS)
        send_all(server, messages)


if __name__ == "__main__":
    main()
