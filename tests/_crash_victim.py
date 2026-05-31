"""A real subprocess that parks a nudge_workflow, then idles until SIGKILLed.

Used by test_zz_crash_resume to simulate a genuine crash: when this process is killed
with SIGKILL, its threads die with it (unlike an in-process DBOS.destroy(), which cannot
interrupt a workflow blocked in recv). The parent test then recovers the orphaned PENDING
workflow and drives it to completion.

Run as:  python -m tests._crash_victim <task_id> <wf_id>
DB URLs are inherited from the parent's environment.
"""
import os
import sys
import time

from dbos import DBOS, DBOSConfig, SetWorkflowID
import app.db  # noqa: F401  (creates the datasource from APP_DATABASE_URL)
from app.workflows import nudge_workflow


def main() -> None:
    task_id, wf_id = sys.argv[1], sys.argv[2]
    config: DBOSConfig = {
        "name": "todo-assistant-test",
        "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
        "application_database_url": os.environ["APP_DATABASE_URL"],
        "executor_id": "crash-victim",   # distinct so it doesn't fight the parent's "local"
        "run_admin_server": False,
    }
    DBOS(config=config)
    DBOS.launch()
    with SetWorkflowID(wf_id):
        DBOS.start_workflow(nudge_workflow, task_id)
    time.sleep(2)  # let the workflow reach and park in the Phase 2 recv(1800)
    print("PARKED", flush=True)
    while True:        # idle until the parent SIGKILLs us
        time.sleep(3600)


if __name__ == "__main__":
    main()
