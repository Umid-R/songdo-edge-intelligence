"""Local rule engine + simulated actuator with monotonic-clock command expiry."""
import hashlib
import json
import logging
import operator
import time

from edge.models import EdgeDecision

log = logging.getLogger("edge.decision")

OPS = {">": operator.gt, "<": operator.lt, ">=": operator.ge, "<=": operator.le, "==": operator.eq}


def rule_version(rule):
    """Short hash of a rule's full definition: it changes whenever the rule's config changes."""
    return hashlib.sha256(json.dumps(rule, sort_keys=True).encode()).hexdigest()[:12]


def decision_id_for(event_id, rule_name, version):
    """Deterministic decision ID: the same event + the same rule version always gives the same ID,
    so re-processing an event after a crash cannot create a second command."""
    return hashlib.sha256(f"{event_id}|{rule_name}|{version}".encode()).hexdigest()[:32]


_BOOT_EPOCH = None


def boot_epoch():
    """Identifier of the current monotonic-clock epoch.

    time.monotonic() restarts from (about) zero at every boot, so a command's issued/expires
    values are only comparable inside the boot that issued them. On Linux the kernel's boot_id
    names that epoch exactly; elsewhere the (rounded) wall-clock boot time is used.
    """
    global _BOOT_EPOCH
    if _BOOT_EPOCH is None:
        try:
            with open("/proc/sys/kernel/random/boot_id", encoding="utf-8") as f:
                _BOOT_EPOCH = f.read().strip()
        except OSError:
            _BOOT_EPOCH = "boot-%d" % (int(time.time() - time.monotonic()) // 60)
    return _BOOT_EPOCH


class RuleEngine:
    """rules come from node.yaml. Teams extend this (compact ML, priorities, ...).

    deterministic_ids=True (used by the shared node's Processor) derives each decision_id from
    event_id + rule + rule version instead of a random UUID, which makes re-processing idempotent.
    """

    def __init__(self, rules, deterministic_ids=False):
        self.rules = rules
        self.deterministic_ids = deterministic_ids
        self.versions = [rule_version(r) for r in rules]

    def evaluate(self, event, now_mono=None, age_s=0.0):
        """age_s: how old the observation already is. A command derived from an old reading is
        issued already aged, so a reading replayed after a crash cannot produce a 'fresh' command."""
        now = time.monotonic() if now_mono is None else now_mono
        issued = now - max(0.0, age_s)
        out = []
        for r, ver in zip(self.rules, self.versions):
            if r["sensor"] == event.sensor and OPS[r["op"]](event.value, r["threshold"]):
                d = EdgeDecision(
                    event_id=event.event_id, rule=r["name"], actuator=r["actuator"],
                    command_value=r["command"], issued_mono=issued,
                    expires_mono=issued + float(r["ttl_s"]), rule_version=ver,
                    boot_epoch=boot_epoch())
                if self.deterministic_ids:
                    d.decision_id = decision_id_for(event.event_id, r["name"], ver)
                out.append(d)
        return out


def dispatch(decision, now_mono=None):
    """Simulated actuator. A stale command (monotonic clock past expiry) is rejected."""
    if not decision.is_fresh(now_mono):
        log.warning("REJECT stale command %s=%s (rule %s)",
                    decision.actuator, decision.command_value, decision.rule)
        return "EXPIRED"
    log.info("ACTUATE %s=%s (rule %s)", decision.actuator, decision.command_value, decision.rule)
    return "DISPATCHED"
