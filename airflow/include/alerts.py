"""
alerts.py — Airflow failure callback and notification helpers.

Used as on_failure_callback in all DAGs.
Supports Slack webhook and email alerts.
"""
from __future__ import annotations

import os
from datetime import datetime

import structlog

log = structlog.get_logger(__name__)

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def on_failure_slack_alert(context: dict) -> None:
    """
    Airflow on_failure_callback that sends a Slack notification.
    
    Attach to any DAG or task as: on_failure_callback=on_failure_slack_alert
    """
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    execution_date = context.get("execution_date", datetime.utcnow())
    log_url = context.get("task_instance").log_url if context.get("task_instance") else ""
    exception = context.get("exception", "No exception info")

    message = {
        "text": (
            f"❌ *Airflow Task Failed*\n"
            f"• *DAG*: `{dag_id}`\n"
            f"• *Task*: `{task_id}`\n"
            f"• *Execution date*: {execution_date}\n"
            f"• *Error*: `{str(exception)[:200]}`\n"
            f"• <{log_url}|View logs>"
        )
    }

    if SLACK_WEBHOOK_URL:
        try:
            import urllib.request
            import json
            data = json.dumps(message).encode("utf-8")
            req = urllib.request.Request(
                SLACK_WEBHOOK_URL,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            log.info("alert.slack_sent", dag=dag_id, task=task_id)
        except Exception as e:
            log.error("alert.slack_failed", error=str(e))
    else:
        log.warning(
            "alert.no_webhook",
            dag=dag_id,
            task=task_id,
            message=message["text"],
        )
