from datetime import datetime, timedelta
from dbos import DBOS
import app.db as db
from app.notifications import send_notification

ACTION_TOPIC = "action"


def _handle_event(task_id: str, event: dict) -> str:
    """Apply an action to the DB. Returns 'snooze' (re-loop) or 'exit'."""
    action = event.get("action")
    if action == "done":
        db.set_status(task_id, "done")
        return "exit"
    if action == "drop":
        db.set_status(task_id, "dropped")
        return "exit"
    if action == "snooze":
        db.set_deadline(task_id, event["new_deadline"])  # sets status back to pending
        return "snooze"
    return "exit"


@DBOS.workflow()
def nudge_workflow(task_id: str) -> None:
    while True:
        task = db.get_task(task_id)
        if task is None or task["status"] != "pending":
            return
        deadline = task["deadline"]

        # Phase 1: durable interruptible wait until T-1h
        delay = max(0.0, (deadline - timedelta(hours=1) - db.utcnow()).total_seconds())
        event = DBOS.recv(ACTION_TOPIC, timeout_seconds=delay)
        if event is not None:
            if _handle_event(task_id, event) == "snooze":
                continue
            return

        # Phase 2: first nudge, 30-minute grace
        send_notification(task_id, "Heads up: deadline in 1h")
        event = DBOS.recv(ACTION_TOPIC, timeout_seconds=1800)
        if event is not None:
            if _handle_event(task_id, event) == "snooze":
                continue
            return

        # Phase 3: escalate, wait until deadline
        send_notification(task_id, "STILL pending. Do it or drop it.", urgent=True)
        delay = max(0.0, (deadline - db.utcnow()).total_seconds())
        event = DBOS.recv(ACTION_TOPIC, timeout_seconds=delay)
        if event is not None:
            if _handle_event(task_id, event) == "snooze":
                continue
            return

        # Phase 4: missed it
        db.set_status(task_id, "overdue")
        return


def _send_summary(title: str, tasks: list[dict]) -> None:
    if not tasks:
        send_notification("scheduled", f"{title} (nothing)")
        return
    lines = [title] + [f"  - [{t['status']}] {t['text']} (due {t['deadline']})" for t in tasks]
    send_notification("scheduled", "\n".join(lines))


@DBOS.scheduled("0 8 * * *")  # daily 8 AM
@DBOS.workflow()
def daily_morning(scheduled_time: datetime, actual_time: datetime) -> None:
    due_soon = [t for t in db.list_tasks(status="pending")
                if t["deadline"] <= db.utcnow() + timedelta(hours=24)]
    overdue = db.list_tasks(status="overdue")
    _send_summary("Morning! Today + overdue:", due_soon + overdue)


@DBOS.scheduled("0 18 * * SUN")  # Sunday 6 PM
@DBOS.workflow()
def weekly_review(scheduled_time: datetime, actual_time: datetime) -> None:
    this_week = db.list_tasks(status="pending", horizon="this_week")
    due_week = [t for t in db.list_tasks(status="pending")
                if t["deadline"] <= db.utcnow() + timedelta(days=7)]
    merged = {t["id"]: t for t in (this_week + due_week)}  # de-dup by id
    _send_summary("Week ahead:", list(merged.values()))


@DBOS.scheduled("0 9 1 * *")  # 1st of month, 9 AM
@DBOS.workflow()
def monthly_checkin(scheduled_time: datetime, actual_time: datetime) -> None:
    this_month = db.list_tasks(status="pending", horizon="this_month")
    _send_summary("New month. Big picture:", this_month)
