import os
import uuid
from datetime import timedelta
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from typing import Optional

# Env defaults for local dev (overridden by Makefile / real env).
# DBOS keeps its workflow state in a SEPARATE system database from the app's todo.tasks.
os.environ.setdefault("APP_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/todo")
os.environ.setdefault("DBOS_SYSTEM_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/todo_dbos_sys")

from dbos import DBOS, DBOSConfig
import app.db as db
import app.workflows  # noqa: F401  (registers workflows)
from app.workflows import ACTION_TOPIC
from app.schemas import CreateTask, SnoozeBody, TaskOut

app = FastAPI(title="Todo Assistant")


@app.get("/health")
async def health():
    return {"ok": True}


_UI_PAGE = (Path(__file__).resolve().parent / "static" / "index.html").read_text()


@app.get("/", response_class=HTMLResponse)
async def index():
    return _UI_PAGE


@app.post("/tasks", response_model=TaskOut)
async def create_task(body: CreateTask):
    from dbos import SetWorkflowID
    from app.workflows import nudge_workflow

    task_id = "task_" + uuid.uuid4().hex
    db.insert_task(task_id, body.text, body.horizon, body.deadline_iso)

    wf_id = f"nudge-{task_id}"
    with SetWorkflowID(wf_id):
        DBOS.start_workflow(nudge_workflow, task_id)  # idempotent via deterministic id
    db.set_workflow_id(task_id, wf_id)

    return TaskOut(**db.get_task(task_id))


@app.get("/tasks", response_model=list[TaskOut])
async def list_tasks(status: Optional[str] = Query(None), horizon: Optional[str] = Query(None)):
    return [TaskOut(**t) for t in db.list_tasks(status=status, horizon=horizon)]


async def _send_action(task_id: str, payload: dict) -> dict:
    task = db.get_task(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    if not task["workflow_id"]:
        raise HTTPException(409, "task has no running workflow")
    # send_async: DBOS forbids the sync send() inside a running event loop (async route).
    await DBOS.send_async(task["workflow_id"], payload, topic=ACTION_TOPIC)
    return task


@app.post("/tasks/{task_id}/done")
async def mark_done(task_id: str):
    await _send_action(task_id, {"action": "done"})
    return {"ok": True}


@app.post("/tasks/{task_id}/drop")
async def drop_task(task_id: str):
    await _send_action(task_id, {"action": "drop"})
    return {"ok": True}


@app.post("/tasks/{task_id}/snooze")
async def snooze_task(task_id: str, body: SnoozeBody):
    task = db.get_task(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    new_deadline = task["deadline"] + timedelta(hours=body.delay_hours)
    await _send_action(task_id, {"action": "snooze", "new_deadline": new_deadline})
    return {"ok": True, "new_deadline": new_deadline.isoformat()}


def main():
    config: DBOSConfig = {
        "name": "todo-assistant",
        "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
        "application_database_url": os.environ["APP_DATABASE_URL"],
    }
    db.apply_migration()   # idempotent; makes the container/local boot self-sufficient
    DBOS(config=config)
    DBOS.launch()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
