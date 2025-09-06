# include/notifications.py

import smtplib
import logging
import os
import yaml
from email.message import EmailMessage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load email config
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

email_conf = load_config()['email']

def send_email(subject, body):
    try:
        msg = EmailMessage()
        msg["From"] = email_conf['from']
        msg["To"] = ", ".join(email_conf['to'])
        msg["Subject"] = subject
        msg.set_content(body)

        with smtplib.SMTP(email_conf['smtp_server'], email_conf['smtp_port']) as server:
            server.starttls()
            server.login(email_conf['smtp_user'], email_conf['smtp_pass'])
            server.send_message(msg)

        logger.info(" Notification sent.")
    except Exception as e:
        logger.error(f" Email failed: {e}")

def notify_task_success(context):
    task = context['task_instance'].task_id
    send_email(
        subject=f"[ SUCCESS] Task '{task}'",
        body=f"The task '{task}' in DAG '{context['dag'].dag_id}' succeeded."
    )

def notify_task_failure(context):
    task = context['task_instance'].task_id
    send_email(
        subject=f"[ FAILED] Task '{task}'",
        body=f"The task '{task}' in DAG '{context['dag'].dag_id}' failed.\n\nError:\n{context['exception']}"
    )

def notify_dag_start():
    send_email(
        subject=f"[ STARTED] DAG has started",
        body=f"The DAG has started running."
    )

def notify_dag_complete(context):
    send_email(
        subject=f"[ COMPLETED] DAG '{context['dag'].dag_id}'",
        body=f"DAG '{context['dag'].dag_id}' has completed successfully."
    )
