"""Optional evidence files written by the T1/T2/T4 tests.

When SONGDO_EVIDENCE_DIR is set (scripts/make_run_bundle.py sets it to <bundle>/test_evidence), the
tests keep what the assessment asks to retain: T1 ID manifests and the recovery journal, T2 the
request/response trace across the receiver, T4 the API transcript. Without the variable, nothing is
written and the tests behave exactly the same.
"""
import json
import os


def enabled():
    return bool(os.environ.get("SONGDO_EVIDENCE_DIR"))


def save(name, obj):
    d = os.environ.get("SONGDO_EVIDENCE_DIR")
    if not d:
        return None
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)
    return path
