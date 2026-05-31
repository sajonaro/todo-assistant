import os
import sys
import uuid
from datetime import timedelta
import app.db as db
from app.workflows import nudge_workflow, ACTION_TOPIC


def _new_id() -> str:
    return "task_" + uuid.uuid4().hex


def test_done_event_exits_workflow_cleanly():
    from dbos import DBOS, SetWorkflowID

    tid = _new_id()
    # Deadline 1h out so Phase 1 waits ~0s then the workflow nudges and parks in Phase 2.
    db.insert_task(tid, "respond test", "today", db.utcnow() + timedelta(hours=1))
    wf_id = f"nudge-{tid}"
    with SetWorkflowID(wf_id):
        handle = DBOS.start_workflow(nudge_workflow, tid)

    # Act: user marks done -> wake the parked recv.
    DBOS.send(wf_id, {"action": "done"}, topic=ACTION_TOPIC)
    handle.get_result()  # workflow should exit, not hang

    assert db.get_task(tid)["status"] == "done"


def test_drop_event_exits_workflow_cleanly():
    # Note: the true 'overdue' transition (Phase 4) only fires after Phase 2's 30-minute
    # grace elapses with no response, so it is verified manually rather than in a fast
    # unit test. Here we verify the drop action exits the workflow and sets status.
    from dbos import DBOS, SetWorkflowID

    tid = _new_id()
    db.insert_task(tid, "drop test", "today", db.utcnow() + timedelta(hours=1))
    wf_id = f"nudge-{tid}"
    with SetWorkflowID(wf_id):
        handle = DBOS.start_workflow(nudge_workflow, tid)  # parks in Phase 2 recv
    DBOS.send(wf_id, {"action": "drop"}, topic=ACTION_TOPIC)
    handle.get_result()
    assert db.get_task(tid)["status"] == "dropped"


def test_crash_resume():
    """Prove a parked workflow survives a genuine crash and still completes.

    A real child process parks the nudge_workflow, then we SIGKILL it (a true crash: its
    threads die with it). This process then recovers the orphaned PENDING workflow and
    drives it to completion via a send.  We use a real subprocess rather than an in-process
    DBOS.destroy() because destroy() cannot interrupt a workflow blocked in recv(1800s) --
    that would strand a non-daemon thread and hang the test process.
    """
    import subprocess
    import time as _time
    from dbos import DBOS

    tid = _new_id()
    db.insert_task(tid, "crash test", "today", db.utcnow() + timedelta(hours=1))
    wf_id = f"nudge-{tid}"

    victim = subprocess.Popen(
        [sys.executable, "-m", "tests._crash_victim", tid, wf_id],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=os.environ.copy(),
    )
    try:
        # Wait until the child reports the workflow is parked.
        parked, start = False, _time.time()
        while _time.time() - start < 30:
            line = victim.stdout.readline()
            if not line:
                if victim.poll() is not None:
                    break
                continue
            if "PARKED" in line:
                parked = True
                break
        assert parked, "victim subprocess did not park the workflow"

        # Sanity: the workflow really is PENDING in the system DB before we crash it.
        # (The kill below is the actual crash.)
        victim.kill()
        victim.wait(timeout=10)
    finally:
        if victim.poll() is None:
            victim.kill()

    # Recover the orphaned workflow in THIS process and drive it to done.
    handle = DBOS.resume_workflow(wf_id)        # resumes from its last checkpoint
    DBOS.send(wf_id, {"action": "done"}, topic=ACTION_TOPIC)
    handle.get_result()

    assert db.get_task(tid)["status"] == "done"


def test_snooze_updates_deadline_and_keeps_workflow():
    from dbos import DBOS, SetWorkflowID

    tid = _new_id()
    original = db.utcnow() + timedelta(hours=1)
    db.insert_task(tid, "snooze test", "today", original)
    wf_id = f"nudge-{tid}"
    with SetWorkflowID(wf_id):
        handle = DBOS.start_workflow(nudge_workflow, tid)  # parks in Phase 2

    new_deadline = original + timedelta(hours=24)
    DBOS.send(wf_id, {"action": "snooze", "new_deadline": new_deadline}, topic=ACTION_TOPIC)
    # The loop consumes snooze (re-loops with new deadline, parks again), then done exits.
    DBOS.send(wf_id, {"action": "done"}, topic=ACTION_TOPIC)
    handle.get_result()

    task = db.get_task(tid)
    assert task["status"] == "done"
    assert task["deadline"] >= original + timedelta(hours=23)  # snooze pushed the deadline
