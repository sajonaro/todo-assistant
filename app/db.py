import os
from datetime import UTC, datetime
from pathlib import Path

from dbos import SQLAlchemyDatasource
from sqlalchemy import create_engine
from sqlalchemy import text as sa_text

ds = SQLAlchemyDatasource.create(os.environ["APP_DATABASE_URL"])

_MIGRATION = Path(__file__).resolve().parent.parent / "migrations" / "001_init.sql"


def utcnow() -> datetime:
    return datetime.now(UTC)


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


def _update_task(task_id: str, **cols) -> None:
    """Set the given columns (TRUSTED names only) + updated_at on one task.
    Column names are interpolated, so never pass a user-supplied key here."""
    assignments = ", ".join(f"{c} = :{c}" for c in cols)
    ds.sql_session().execute(
        sa_text(f"UPDATE todo.tasks SET {assignments}, updated_at = now() WHERE id = :id"),
        {**cols, "id": task_id},
    )


@ds.transaction()
def set_workflow_id(task_id: str, workflow_id: str) -> None:
    _update_task(task_id, workflow_id=workflow_id)


@ds.transaction()
def get_task(task_id: str) -> dict | None:
    row = ds.sql_session().execute(
        sa_text("SELECT * FROM todo.tasks WHERE id = :id"), {"id": task_id}
    ).mappings().first()
    return dict(row) if row else None


@ds.transaction()
def list_tasks(status: str | None = None, horizon: str | None = None) -> list[dict]:
    filters = {k: v for k, v in {"status": status, "horizon": horizon}.items() if v}
    where = (" WHERE " + " AND ".join(f"{k} = :{k}" for k in filters)) if filters else ""
    rows = ds.sql_session().execute(
        sa_text(f"SELECT * FROM todo.tasks{where} ORDER BY deadline ASC"), filters
    ).mappings().all()
    return [dict(r) for r in rows]


@ds.transaction()
def set_status(task_id: str, status: str) -> None:
    _update_task(task_id, status=status)


@ds.transaction()
def set_deadline(task_id: str, deadline: datetime) -> None:
    _update_task(task_id, deadline=deadline, status="pending")
