import os

import pytest

# Separate system DB from app DB: reset_system_database() drops/recreates the SYSTEM
# database, so it must NOT be the same database that holds todo.tasks.
os.environ.setdefault("APP_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/todo")
os.environ.setdefault("DBOS_SYSTEM_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/todo_dbos_sys")

from dbos import DBOS, DBOSConfig  # noqa: E402

import app.db as db  # noqa: E402  (registers @ds.transaction functions)
import app.workflows  # noqa: E402,F401  (registers workflows once they exist)


@pytest.fixture(scope="session", autouse=True)
def dbos_runtime():
    config: DBOSConfig = {
        "name": "todo-assistant-test",
        "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
        "application_database_url": os.environ["APP_DATABASE_URL"],
    }
    db.apply_migration()   # ensure todo.tasks exists (no separate `make migrate` needed)
    DBOS(config=config)
    # Clear any workflows left PENDING by a prior (possibly killed) run. Otherwise DBOS
    # recovery resurrects them on launch; a nudge_workflow parked in recv(timeout=1800)
    # holds a non-daemon executor thread that destroy() can't interrupt, so the test
    # process never exits. reset_system_database() is DBOS's official test-reset hook.
    DBOS.reset_system_database()
    DBOS.launch()
    yield
    DBOS.destroy()


@pytest.fixture(autouse=True)
def clean_tasks():
    from sqlalchemy import text as sa_text

    @db.ds.transaction()
    def _truncate():
        db.ds.sql_session().execute(sa_text("TRUNCATE todo.tasks"))

    _truncate()
    yield
