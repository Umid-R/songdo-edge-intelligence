"""Local rule engine + simulated actuator with monotonic-clock command expiry."""
import logging
import operator
import time

from edge.models import EdgeDecision

log = logging.getLogger("edge.decision")

OPS = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq}


class RuleEngine:
    """rules come from node.yaml. Teams extend this (compact ML, priorities, ...)."""

    def __init__(self, rules):
        self.rules = rules

    def evaluate(self, event, now_mono=None):
        now = time.monotonic() if now_mono is None else now_mono
        out = []
        for r in self.rules:
            if r["sensor"] == event.sensor and OPS[r["op"]](event.value, r["threshold"]):
                out.append(EdgeDecision(
                    event_id=event.event_id, rule=r["name"], actuator=r["actuator"],
                    command_value=r["command"], issued_mono=now,
                    expires_mono=now + float(r["ttl_s"])))
        return out


def dispatch(decision, now_mono=None):
    """Simulated actuator. A stale command (monotonic clock past expiry) is rejected."""
    if not decision.is_fresh(now_mono):
        log.warning("REJECT stale command %s=%s (rule %s)",
                    decision.actuator, decision.command_value, decision.rule)
        return "EXPIRED"
    log.info("ACTUATE %s=%s (rule %s)", decision.actuator, decision.command_value, decision.rule)
    return "DISPATCHED"
