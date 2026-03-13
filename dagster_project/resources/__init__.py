import smtplib
from email.mime.text import MIMEText
import os


def send_alert_email(subject: str, body: str):
    sender = os.getenv("ALERT_EMAIL_FROM", "ursuser@gmail.com")
    recipient = os.getenv("ALERT_EMAIL_TO", "ursuser@gmail.com")
    password = os.getenv("ALERT_EMAIL_PASSWORD", "")

    msg = MIMEText(body)
    msg["subject"] = subject
    msg["from"] = sender
    msg["to"] = recipient

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
