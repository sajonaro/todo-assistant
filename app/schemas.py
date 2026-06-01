from datetime import datetime
from typing import Literal

from pydantic import BaseModel

Horizon = Literal["today", "this_week", "this_month"]
Status = Literal["pending", "done", "snoozed", "dropped", "overdue"]


class CreateTask(BaseModel):
    text: str
    horizon: Horizon
    deadline_iso: datetime  # pydantic parses ISO-8601 strings into datetime


class SnoozeBody(BaseModel):
    delay_hours: float = 24


class TaskOut(BaseModel):
    id: str
    text: str
    horizon: Horizon
    deadline: datetime
    status: Status
    workflow_id: str | None = None
