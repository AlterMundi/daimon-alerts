"""Project bounded DMK evidence for the existing restricted Reports reader."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import time


def read_receipt(path, schema=None):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return {}
            raw = stream.read(65537)
        value = json.loads(raw) if len(raw) <= 65536 else None
        if isinstance(value, dict) and (schema is None or value.get("schema") == schema):
            return value
    except (OSError, ValueError):
        pass
    return {}


def label(value, allowed):
    return value if isinstance(value, str) and value in allowed else "unobserved"


def count(value):
    return value if type(value) is int and 0 <= value <= 2**63 - 1 else None


def field(value, key):
    item = value.get(key)
    return item if isinstance(item, dict) else {}


def project(worker, incident, learning, peer, supervisor, now, freshness):
    phases = field(worker, "phases")
    capture, index, organization = (field(phases, phase) for phase in
                                    ("capture", "index", "organization"))
    backup = field(worker, "backup")
    states = {"idle", "running", "complete", "pending", "deferred"}
    started = count(worker.get("started_ns"))
    updated = count(worker.get("updated_ns"))
    updated = updated // 10**9 if updated is not None else None
    age = now - updated if updated is not None else None
    worker_freshness = ("unobserved" if age is None or age < -60 else
                        "stale" if age > freshness else "fresh")
    observed_learning = (
        learning.get("phase") == "own_source_skill_behavior_and_memory_application_verified"
        and field(field(learning, "memory"), "full_native_read").get("complete") is True
    )
    observed_peer = (
        peer.get("phase") == "automatic_selected_request_evaluated_and_correlated_defer_received"
        and peer.get("response_context_matches_original") is True
        and peer.get("decision") == "defer-new-DMK-entry"
    )
    semantic = {
        "worker_freshness": worker_freshness,
        "worker_state": label(worker.get("state"), states),
        "capture": {
            "state": label(capture.get("state"), states),
            "processed_through": count(capture.get("processed_through")),
            "reason": label(capture.get("reason_code"), {
                "source_context_budget_exceeded", "navigation_budget_exhausted", "waiting_for_capture",
                "dispatch_unresolved"}),
            "minimum_required_bytes": count(capture.get("minimum_required_bytes")),
            "proposer_invoked": capture.get("proposer_invoked") if type(capture.get("proposer_invoked")) is bool else None,
        },
        "index": {"state": label(index.get("state"), states),
                  "indexed_through": count(index.get("indexed_through")),
                  "organization_indexed_through": count(index.get("organization_indexed_through"))},
        "organization": {"state": label(organization.get("state"), states),
                         "reason": label(organization.get("reason_code"), {"accounts_current"}),
                         "organized_through": count(organization.get("organized_through")),
                         "ready_accounts": count(organization.get("ready_accounts")),
                         "blocked_accounts": count(organization.get("blocked_accounts")),
                         "applied_consolidation_receipt": "not_selected"},
        "backup": {"state": label(backup.get("state"), {"current", "stale", "pending", "failed"}),
                   "verified_baselines": count(backup.get("verified_baselines"))},
        "dmk_alerts_supervisor": {
            "unit_state": label(supervisor, {"active", "activating", "inactive", "failed"}),
            "incident_state": label(incident.get("processing_state"), {"open", "resolved"}),
            "problem": label(incident.get("problem_code"), {
                "worker_stalled", "worker_stopped", "observation_stale", "observation_unreadable"}),
        },
        "learning": {
            "selected_local_use": "verified_at_checkpoint" if observed_learning else "unobserved",
            "peer_assessment": "correlated_defer" if observed_peer else "unobserved",
            "recurrent_same_being_use": "unverified",
            "sustained_reciprocal_use": "unverified",
        },
    }
    revision = hashlib.sha256(json.dumps(semantic, sort_keys=True).encode()).hexdigest()
    return {
        "schema": "daimon-alerts.dmk-reports-health/v1",
        "checked_at": now,
        "source_times": {"worker_started_ns": started, "worker_updated_at": updated,
                         "backup_verified_ns": count(backup.get("last_verified_ns")),
                         "supervisor_incident_updated_at": count(incident.get("updated_at")),
                         "selected_local_use_at_ms": count(learning.get("at_ms")) if observed_learning else None,
                         "peer_assessment_at_ms": count(peer.get("at_ms")) if observed_peer else None},
        "meaningful_revision": revision,
        "evidence": semantic,
    }


def supervisor_state(unit):
    try:
        result = subprocess.run(
            ["/usr/bin/systemctl", "--user", "show", unit, "-p", "ActiveState", "--value"],
            text=True, capture_output=True, timeout=3, check=True,
        )
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unobserved"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field_name in ("worker", "incident", "learning", "peer", "output"):
        parser.add_argument("--" + field_name, required=True, type=Path)
    parser.add_argument("--supervisor", required=True)
    parser.add_argument("--freshness-seconds", required=True, type=int, choices=range(1, 172801),
                        metavar="SECONDS")
    args = parser.parse_args()
    directory_stat = args.output.parent.lstat()
    if (not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_uid != os.getuid()
            or directory_stat.st_mode & 0o077):
        raise SystemExit("Owner-private projection directory required")
    result = project(read_receipt(args.worker, "hmk.worker-status/v1"), read_receipt(args.incident),
                     read_receipt(args.learning), read_receipt(args.peer),
                     supervisor_state(args.supervisor), int(time.time()), args.freshness_seconds)
    fd, temporary = tempfile.mkstemp(dir=args.output.parent, prefix=".dmk-reports-health-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(result, stream, sort_keys=True)
            stream.write("\n")
        os.replace(temporary, args.output)
    finally:
        Path(temporary).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
