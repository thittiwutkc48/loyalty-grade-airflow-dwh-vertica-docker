import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from datetime import datetime, timedelta


# SendGrid email settings
SENDGRID_API_KEY = "your-sendgrid-api-key"  # Replace with your SendGrid API Key
FROM_EMAIL = "your-email@example.com"  # Replace with the email address you want to send from
TO_EMAIL = "recipient-email@example.com"  # Replace with the recipient's email address

# Email sending function (using SendGrid API)
def send_email(subject, body):
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=TO_EMAIL,
        subject=subject,
        plain_text_content=body
    )
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print(f"Email sent successfully. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error sending email: {e}")

# Success callback function
def success_callback(context):
    dag_run = context.get("dag_run")
    subject = f"DAG {dag_run.dag_id} - SUCCESS"
    body = f"The DAG {dag_run.dag_id} ran successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}."
    send_email(subject, body)

# Failure callback function
def failure_callback(context):
    dag_run = context.get("dag_run")
    subject = f"DAG {dag_run.dag_id} - FAILURE"
    body = f"The DAG {dag_run.dag_id} failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.\n\nDetails:\n{str(context.get('exception'))}"
    send_email(subject, body)
