from datetime import datetime, timedelta

from dbos import DBOS

import app.db as db
from app.notifications import send_notification

ACTION_TOPIC = "action"
_TERMINAL = {"done": "done", "drop": "dropped"}  # action -> status


def _handle_event(task_id: str, event: dict) -> str:
    """Apply an action to the DB. Returns 'snooze' (re-loop) or 'exit'."""
    action = event.get("action")
    if action == "snooze":
        db.set_deadline(task_id, event["new_deadline"])  # sets status back to pending
        return "snooze"
    if action in _TERMINAL:
        db.set_status(task_id, _TERMINAL[action])
    return "exit"


def _await_action(task_id: str, timeout: float) -> str | None:
    """Block for an action. Returns None on timeout, else 'snooze'/'exit'."""
    event = DBOS.recv(ACTION_TOPIC, timeout_seconds=max(0.0, timeout))
    return _handle_event(task_id, event) if event is not None else None


@DBOS.workflow()
def nudge_workflow(task_id: str) -> None:
    while True:
        task = db.get_task(task_id)
        if task is None or task["status"] != "pending":
            return
        deadline = task["deadline"]
        # (notification-or-None, lambda -> seconds to wait). Lambdas re-read the clock
        # per phase, so each wait targets the right moment even after earlier waits.
        # d=deadline binds by value (deadline is stable within an iteration; rebuilt on re-loop).
        phases = [
            (None, lambda d=deadline: (d - timedelta(hours=1) - db.utcnow()).total_seconds()),
            (("Heads up: deadline in 1h", False), lambda: 1800.0),
            (("STILL pending. Do it or drop it.", True), lambda d=deadline: (d - db.utcnow()).total_seconds()),
        ]
        for notify, timeout in phases:
            if notify:
                send_notification(task_id, notify[0], urgent=notify[1])
            outcome = _await_action(task_id, timeout())
            if outcome == "exit":
                return
            if outcome == "snooze":
                break  # restart the while-loop with the refreshed deadline
        else:
            db.set_status(task_id, "overdue")  # no break -> every phase timed out
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
