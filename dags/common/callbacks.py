"""Airflow failure callback: sends email via SMTP_SSL if SMTP host is configured."""
from __future__ import annotations

import logging
import os
import smtplib
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def notify_on_failure(context: dict) -> None:
    smtp_host = os.environ.get("AIRFLOW__SMTP__SMTP_HOST")
    if not smtp_host:
        logger.warning("notify_on_failure: AIRFLOW__SMTP__SMTP_HOST not set, skipping email")
        return

    dag_id = getattr(context.get("dag"), "dag_id", "?")
    ti = context.get("task_instance")
    task_id = getattr(ti, "task_id", "?")
    log_url = getattr(ti, "log_url", "")
    execution_date = context.get("execution_date", "?")
    exception = context.get("exception", "")

    body = (
        f"DAG:       {dag_id}\n"
        f"Task:      {task_id}\n"
        f"Date:      {execution_date}\n"
        f"Log:       {log_url}\n"
        f"Exception: {exception}\n"
    )

    mail_from = os.environ.get("AIRFLOW__SMTP__SMTP_MAIL_FROM", "airflow@geoplatform.local")
    mail_to = os.environ.get("AIRFLOW_ALERT_EMAIL", mail_from)
    smtp_port = int(os.environ.get("AIRFLOW__SMTP__SMTP_PORT", "465"))
    smtp_user = os.environ.get("AIRFLOW__SMTP__SMTP_USER", "")
    smtp_password = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD", "")

    msg = MIMEText(body)
    msg["Subject"] = f"[Airflow FAIL] {dag_id}.{task_id}"
    msg["From"] = mail_from
    msg["To"] = mail_to

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.sendmail(mail_from, [mail_to], msg.as_string())
    except Exception as exc:
        logger.error("notify_on_failure: failed to send email: %s", exc)
