import os
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

# Env defaults for local dev (overridden by Makefile / real env).
# DBOS keeps its workflow state in a SEPARATE system database from the app's todo.tasks.
os.environ.setdefault("APP_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/todo")
os.environ.setdefault("DBOS_SYSTEM_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/todo_dbos_sys")

from dbos import DBOS, DBOSConfig, SetWorkflowID

import app.db as db
from app.schemas import CreateTask, SnoozeBody, TaskOut
from app.workflows import ACTION_TOPIC, nudge_workflow  # importing also registers all workflows

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
    task_id = "task_" + uuid.uuid4().hex
    db.insert_task(task_id, body.text, body.horizon, body.deadline_iso)

    wf_id = f"nudge-{task_id}"
    with SetWorkflowID(wf_id):
        DBOS.start_workflow(nudge_workflow, task_id)  # idempotent via deterministic id
    db.set_workflow_id(task_id, wf_id)

    return TaskOut(**db.get_task(task_id))


@app.get("/tasks", response_model=list[TaskOut])
async def list_tasks(status: str | None = Query(None), horizon: str | None = Query(None)):
    return [TaskOut(**t) for t in db.list_tasks(status=status, horizon=horizon)]


def _require_task(task_id: str) -> dict:
    task = db.get_task(task_id)
    if task is None:
        raise HTTPException(404, "task not found")
    return task


async def _send_action(task: dict, payload: dict) -> None:
    if not task["workflow_id"]:
        raise HTTPException(409, "task has no running workflow")
    # send_async: DBOS forbids the sync send() inside a running event loop (async route).
    await DBOS.send_async(task["workflow_id"], payload, topic=ACTION_TOPIC)


# Declared before the generic /{action} route below so it matches first.
@app.post("/tasks/{task_id}/snooze")
async def snooze_task(task_id: str, body: SnoozeBody):
    task = _require_task(task_id)
    new_deadline = task["deadline"] + timedelta(hours=body.delay_hours)
    await _send_action(task, {"action": "snooze", "new_deadline": new_deadline})
    return {"ok": True, "new_deadline": new_deadline.isoformat()}


@app.post("/tasks/{task_id}/{action}")
async def act(task_id: str, action: Literal["done", "drop"]):
    await _send_action(_require_task(task_id), {"action": action})
    return {"ok": True}


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
