"""Shared data models. Agree as a team before changing any field."""
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Optional


def new_id():
    return uuid.uuid4().hex


@dataclass
class SensorEvent:
    node_id: str
    sensor: str
    value: float
    unit: str = ""
    observed_at: float = field(default_factory=time.time)   # event time
    received_at: Optional[float] = None                     # receipt time
    event_id: str = field(default_factory=new_id)           # idempotency key

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


@dataclass
class EdgeDecision:
    event_id: str
    rule: str
    actuator: str
    command_value: float
    issued_mono: float            # time.monotonic() when the command was issued
    expires_mono: float           # issued_mono + ttl
    decision_id: str = field(default_factory=new_id)
    status: str = "PENDING"       # PENDING | DISPATCHED | EXPIRED
    created_at: float = field(default_factory=time.time)

    def is_fresh(self, now_mono=None):
        """A command is stale once the monotonic clock passes expires_mono."""
        now = time.monotonic() if now_mono is None else now_mono
        return now < self.expires_mono

    def to_dict(self):
        return asdict(self)


@dataclass
class ProcessHealth:
    name: str
    pid: int
    state: str
    rss_kb: int
    cpu_pct: float
    ctx_voluntary: int
    ctx_involuntary: int
    restarts: int = 0
    updated_at: float = field(default_factory=time.time)

    def to_dict(self):
        return asdict(self)
