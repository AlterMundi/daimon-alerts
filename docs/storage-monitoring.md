# Filesystem capacity producer

`storage_monitor.py` is a finite, standard-library `statvfs` probe. It performs
no deletion, recursive size scan, model call, inbox read or recovery action.
The existing Rust incident core and receiving Telegram/Sessions adapter own
persistence and delivery. Python reuses the maintained authenticated local client;
there is no second incident service or bot consumer.

Install the sample storage service/timer only under an authorized monitoring
request. Configure owner-private `storage.json` with `base_url`, `token_file`,
`state_dir`, `producer_id`, `observer_id`, `receiving_scope`, `thresholds`, and
`targets`. Each target has a unique opaque `id` (at most 70 characters), an
absolute mount `path`, and a catalog `resource_ref`. No local path enters an
incident. A selected path that is no longer a mount reports observation failure
rather than measuring its parent filesystem. Do not select a directory within a
mount. An unavailable inode count remains explicitly unreported.

Example thresholds (percentages and GiB, applied independently to every mount):

```json
{"warning_percent":82,"critical_percent":90,
 "warning_free_gib":15,"critical_free_gib":7,
 "warning_inode_percent":82,"critical_inode_percent":90,
 "recovery_percent":3,"recovery_gib":2}
```

Percent used includes the effect of reserved blocks on unprivileged available
space, matching `df`; available GiB uses `f_bavail`. Any byte or inode threshold
can trigger pressure. Recovery requires the corresponding hysteresis margin,
which prevents boundary oscillation. The monitor observes; cleanup is a separate,
explicitly configured operation. It cannot certify backup validity or growth rate.

Add a dedicated actor with exactly the producer and observer IDs, the receiving
scope and `publish`, `observe`, `status` grants. Use a new owner-private random
token, not an existing producer's credential. Install the core and receiving
contract supporting the additive `storage_pressure` problem code **before**
enabling this producer. Strict older receiving schema copies reject that code.
Preserve the incident database, delivery/ack receipts and all other actor grants
through the Alerts-only restart. Native conversations require no restart.

The private SQLite journal freezes projections before HTTP, retries uncertain
acceptance exactly, resolves at a higher revision and uses a new ID on recurrence.
Metrics refresh locally without incrementing incident revisions on every tick.
Severity changes update the same incident; the v1 delivery contract dispatches
only open/resolved transitions, so escalation alone does not create another
notification. Probe failure has its own incident and cannot resolve pressure.
An unreadable mount is not evidence of healthy storage. Outage freezes queued
transitions until acceptance can be reconciled; no false recovery is published.

Observer heartbeat and incident state are independent. Set the timer's interval
and timeout comfortably inside the installed core's freshness budget. A stale
observer is visible in Sessions; the producer cannot certify its own death.
A separate failure of the entire host/receiver requires external supervision.
Inspect the journal's `status` table for last probe time, metrics and service
availability. An inactive successful oneshot is normal; a running timer alone is
not evidence that the probe succeeded.

Qualification: `cargo build --locked && python3 tests/storage_monitor_io.py`.
These tests use fictional capacity samples and real isolated HTTP/SQLite service
state, including exact replay after lost acceptance. They do not fill a real disk
or dispatch a Telegram notice.

During migration, verify actual probe, accepted incident and receiving visibility
before retiring the matching old scheduled check through its owning CLI. Keep
its configuration and historical deliveries. Do not disable unrelated jobs.
Rollback disables only this timer and selects the preserved core/receiver release
and config; retained journals and incident state are not rolled back. Keep a
compatible receiver while retained storage incidents are still visible.
