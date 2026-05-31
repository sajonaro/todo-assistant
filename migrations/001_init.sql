CREATE SCHEMA IF NOT EXISTS todo;

CREATE TABLE IF NOT EXISTS todo.tasks (
    id              TEXT PRIMARY KEY,
    text            TEXT NOT NULL,
    horizon         TEXT NOT NULL,
    deadline        TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    workflow_id     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tasks_status_deadline_idx ON todo.tasks (status, deadline);
CREATE INDEX IF NOT EXISTS tasks_horizon_status_idx  ON todo.tasks (horizon, status);
