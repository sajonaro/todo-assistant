from dbos import DBOS


@DBOS.step()
def send_notification(task_id: str, message: str, urgent: bool = False) -> None:
    tag = "URGENT" if urgent else "NUDGE"
    print(f"[{tag}] task={task_id} :: {message}", flush=True)
