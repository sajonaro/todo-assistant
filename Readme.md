# Todo Assistant

A proactive task assistant: add a task with a deadline and it runs a durable workflow that
nudges you before the deadline, escalates if you ignore it, and marks it overdue if you miss it.
Built on FastAPI + DBOS + Postgres.

## Run it

```bash
make up          # build + start app and Postgres in containers
```

Then open **http://localhost:8000** for the UI. Stop with `make down`.

## Use the UI

1. Enter a task, pick a horizon (`today` / `this_week` / `this_month`) and a deadline, click **Add task**.
2. The task appears in the list. While it's `pending` you can **Done**, **Snooze 1d**, or **Drop** it.
3. Notifications print to the app log: `docker compose logs -f app`.

## Use the API

```bash
# create
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"text":"file report","horizon":"today","deadline_iso":"2026-06-01T17:00:00Z"}'

# list (optional ?status=pending&horizon=today)
curl localhost:8000/tasks

# act on a task (id from create/list)
curl -X POST localhost:8000/tasks/<id>/done
curl -X POST localhost:8000/tasks/<id>/snooze -H 'content-type: application/json' -d '{"delay_hours":24}'
curl -X POST localhost:8000/tasks/<id>/drop
```

What a task does on its own: nudges ~1h before the deadline, escalates after 30 min of silence,
and marks itself `overdue` if the deadline passes. Snooze pushes the deadline; done/drop end it.

## Develop locally

```bash
make install     # uv: create venv + install deps
make db          # start only Postgres
make run         # run the app via uv (http://localhost:8000)
make test        # run the test suite
```

Don't run `make test` while the container stack is up — both share the same Postgres and the
tests reset DBOS's system database.
