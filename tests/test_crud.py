import uuid
from datetime import timedelta
import app.db as db


def _new_id() -> str:
    return "task_" + uuid.uuid4().hex


def test_insert_and_get_roundtrip():
    tid = _new_id()
    deadline = db.utcnow() + timedelta(hours=2)
    db.insert_task(tid, "buy milk", "today", deadline)

    got = db.get_task(tid)
    assert got is not None
    assert got["text"] == "buy milk"
    assert got["horizon"] == "today"
    assert got["status"] == "pending"
    assert got["workflow_id"] is None


def test_list_filters_by_status_and_horizon():
    a, b = _new_id(), _new_id()
    deadline = db.utcnow() + timedelta(hours=2)
    db.insert_task(a, "task a", "today", deadline)
    db.insert_task(b, "task b", "this_week", deadline)
    db.set_status(b, "done")

    pending_today = db.list_tasks(status="pending", horizon="today")
    assert [t["id"] for t in pending_today] == [a]


def test_set_workflow_id():
    tid = _new_id()
    db.insert_task(tid, "x", "today", db.utcnow() + timedelta(hours=1))
    db.set_workflow_id(tid, "nudge-" + tid)
    assert db.get_task(tid)["workflow_id"] == "nudge-" + tid
