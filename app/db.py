import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from sqlalchemy import create_engine, text as sa_text
from dbos import SQLAlchemyDatasource

ds = SQLAlchemyDatasource.create(os.environ["APP_DATABASE_URL"])

_MIGRATION = Path(__file__).resolve().parent.parent / "migrations" / "001_init.sql"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def apply_migration() -> None:
    """Apply the idempotent app-schema migration. Runs before DBOS.launch(), using a
    throwaway engine independent of the DBOS datasource (which needs launch() first)."""
    # Force the psycopg3 driver (psycopg2 isn't installed; DBOS ships psycopg3).
    url = os.environ["APP_DATABASE_URL"].replace("postgresql://", "postgresql+psycopg://", 1)
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            for stmt in (s.strip() for s in _MIGRATION.read_text().split(";")):
                if stmt:
                    conn.exec_driver_sql(stmt)
    finally:
        engine.dispose()


@ds.transaction()
def insert_task(id: str, text: str, horizon: str, deadline: datetime) -> None:
    ds.sql_session().execute(
        sa_text(
            "INSERT INTO todo.tasks (id, text, horizon, deadline) "
            "VALUES (:id, :text, :horizon, :deadline)"
        ),
        {"id": id, "text": text, "horizon": horizon, "deadline": deadline},
    )


@ds.transaction()
def set_workflow_id(task_id: str, workflow_id: str) -> None:
    ds.sql_session().execute(
        sa_text("UPDATE todo.tasks SET workflow_id = :wf, updated_at = now() WHERE id = :id"),
        {"wf": workflow_id, "id": task_id},
    )


@ds.transaction()
def get_task(task_id: str) -> Optional[dict]:
    row = ds.sql_session().execute(
        sa_text("SELECT * FROM todo.tasks WHERE id = :id"), {"id": task_id}
    ).mappings().first()
    return dict(row) if row else None


@ds.transaction()
def list_tasks(status: Optional[str] = None, horizon: Optional[str] = None) -> list[dict]:
    sql = "SELECT * FROM todo.tasks WHERE 1=1"
    params: dict = {}
    if status:
        sql += " AND status = :status"
        params["status"] = status
    if horizon:
        sql += " AND horizon = :horizon"
        params["horizon"] = horizon
    sql += " ORDER BY deadline ASC"
    rows = ds.sql_session().execute(sa_text(sql), params).mappings().all()
    return [dict(r) for r in rows]


@ds.transaction()
def set_status(task_id: str, status: str) -> None:
    ds.sql_session().execute(
        sa_text("UPDATE todo.tasks SET status = :s, updated_at = now() WHERE id = :id"),
        {"s": status, "id": task_id},
    )


@ds.transaction()
def set_deadline(task_id: str, deadline: datetime) -> None:
    ds.sql_session().execute(
        sa_text(
            "UPDATE todo.tasks SET deadline = :d, status = 'pending', updated_at = now() "
            "WHERE id = :id"
        ),
        {"d": deadline, "id": task_id},
    )
