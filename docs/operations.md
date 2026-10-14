# Installation, supervision and recovery

Build `cargo build --release --locked`; retain the exact source commit, executable
SHA256 and Cargo.lock. Select local loopback capacity and operator scope from the
receiving capability map. The core runtime has no Python dependency. HMK projection
observation uses Python's standard library and the existing HMK classifier/API;
Sessions already uses Python. These adapters avoid reimplementing maintained
producer/authentication contracts in another language.

Use an owner-only config/state directory, umask 0077. Service JSON requires
`database`, `listen`, positive `freshness_seconds`, `max_incidents` (at most 100000),
`retention_seconds` (at least freshness), `actors` and `routes`. Actors contain
`actor_id`, SHA256 of a random >=32-byte token in `token_sha256`, `producers`,
`scopes`, `adapters`, `grants`. Routes contain `receiving_scope`, `adapter_id`.
Keep actual values in private overlays; never commit them. Never reuse a producer
credential for a browser or another being. Token revocation/replacement requires
an Alerts-only process restart; it does not affect producer work or Telecodex.

Install the selected executable and scripts under an immutable operator release
directory, then atomically select `current`. The provided systemd user units are
examples with resource limits. Install/enable only within the authorized receiving
operation. No global Stop is needed for this independent service. Preserve the
existing Telegram consumer, its SQLite store, native goals and conversations.

Observer JSON selects `base_url`, `token_file`, `state_dir`, `hmk_source`, bound
`inbox`, `inbox_binding`, `worker_state_dir`, `producer_id`, `observer_id`,
`receiving_scope`, `resource_ref`, `source_freshness_seconds`, `stalled_seconds`
and optional `worker_unit`, `interval_seconds`. Grant the observer `publish`,
`observe`, `status` and its exact producer/observer/scope associations. Pin the
HMK source containing `provider_status.safe_projection`. Observation never calls
processing, resolves an HMK incident, changes providers or records Telegram
acceptance in the HMK journal. The read-only Inbox facade refuses missing files
and mismatched bindings. HTTP revisions are persisted in the exporter journal
before ingress; restart/lost acceptance safely republishes the same content.

`PUT /v1/observers/{observer_id}` is an operator-only route. Its strict input is
`receiving_scope`, nullable Unix `source_at`, and `source_state` (`healthy`,
`worker_stopped`, `worker_stalled`, `observation_stale`, `observation_unreadable`).
Authentication supplies the observer producer association and scope. Response is
Status. Observer health is distinct from authoritative processing: a pending
provider incident remains open even with a healthy fresh observation route.
Worker running receipts are not liveness proof. Configure the actual supervised
worker unit to detect absence; without one, only source age/stall is observed.
Finite worker completion is not a failed daemon: choose the unit semantics and
freshness policy for the actual producer launcher.

For `memory_runtime.py` supervised by HMK's finite service, select
`worker_mode: "oneshot"`, `worker_unit: "hmk-memory-worker.service"` and
`worker_systemd_scope: "user"` (or `"system"` for a system manager). The observer
checks LoadState, Type, ActiveState, Result, ExecMainCode and ExecMainStatus.
A completed exit 0 with an idle/pending receipt is observable; exit 2 preserves
pending work and is also observable even though systemd marks the unit failed.
Its provider incident remains open. Systemd may discard successful invocation
metadata after a unit becomes inactive; a fresh idle/pending HMK receipt is then
required, and no invocation is inferred from the unit alone. Exit 1, signal death
or an inactive unit with a running receipt requires attention. Missing/mismatched
supervision is unreadable. Receipt age still detects overdue observation;
an active timer does not certify the last worker invocation. Select freshness
for the actual authorized cadence, including expected maximum runtime and grace.
The observer never starts the worker or retries a request.

An absent first worker receipt is explicitly unobserved, including a valid bound
journal whose operational outbox has not yet been initialized. The observer never
creates either HMK artifact. The existing v1 observation-unreadable projection
has a specific not-started/unobserved summary, nullable source time, and private
observer status `observation_kind: "unobserved"`. A failed loaded service before
the first receipt reports worker_stopped with a launch-failure summary instead.
Malformed bindings/journals remain unreadable; their errors are never projected.

For an authorized daily calendar, select `worker_daily_utc: "00:00"` and a positive
`worker_cadence_grace_seconds` (at most 86400). After that grace window, a worker
receipt whose `started_ns` predates the expected invocation reports stale with a
missed-invocation summary. Receipt updates alone cannot hide a missing run.
These cadence fields join the immutable observer binding. This selects observation
policy only; it installs no HMK timer and initiates no recovery or paid request.

## Retire a completed observation fixture

Stop and disable only its selected observation supervisor, then run
`observer.py --config PRIVATE_CONFIG --retire`. The exclusive state lock refuses
an active writer. The helper resolves only its own observation-liveness incident
with an explicit retirement summary, preserves source work and all delivery/ack
receipts, and removes its health registration. It never declares source recovery.
Retired state refuses ordinary ticks until explicit reconciliation; restart does
not silently reactivate a completed fixture.

The operator-only `POST /v1/observers/{observer_id}/retire` takes the closed
`ObserverRetirementInput` (`receiving_scope`) and returns the existing `Status`.
It requires `observe`, `status`, exact observer producer association and scope.
Repeated retirement is idempotent; incidents and delivery intents remain intact.
If no active observers remain, status is `unobserved`, not certified healthy.
This additive v1 route needs no database-format migration. Older cores refuse it;
retain their accepted state and install the qualified compatible successor first.

## Capture before the HMK journal

`scripts/capture_supervisor.py` wraps the existing selected HMK capture API. Use
it only for already-authorized hook events and source binding, preserving their
admission boundary. It reads the small native event envelope with a bounded input wait and writes
the selected source hash and event in the first private durable intent before
starting a bounded synchronous child; it imports no model or canonical-memory writer. Only the
original capture handler reads the selected transcript and writes its existing
inbox. Payloads, transcript paths, source text, credentials and exception messages
never enter the supervision ledger or public projections.

Initialize the private ledger with `--initialize --state-dir ...
--binding-sha256 ...` before installing the selected wrapper. Initialization
records only the selected binding; it does not certify capture or preserved work.

Supply `--binding`, exact file `--binding-sha256`, pinned `--handler-source`,
private `--state-dir` and `--timeout-seconds`. Timeout must fit the native hook's
deadline including wrapper startup, at most one second of input wait and durable writes. Qualify the actual receiving
budget: a 1.5-second child bound failed on the real containers. The qualified
2026-10-14 settings are Oliva child5/outer10 seconds for both events, Eko
Stop15/30 and SessionEnd5/10; CompAII's existing setting remained sufficient.
These are receiving observations, not a universal default. Native kill leaves a durable started intent;
after its persisted deadline the independent observer reports uncertainty.
Failed/unknown attempts are retained. The128-attempt ledger prunes terminal
success/skip entries; resolved failures remain at least24hours. Admission cannot
silently discard unresolved gaps. Missing/unreadable supervision remains explicit.

Configure the observer's optional `capture_state_dir` and
`capture_binding_sha256` for read-only gap projection. These join its immutable
binding; adopt a new private observer state and preserve the existing projection
revisions/accepted receipts when explicitly adding this route. The HTTP schema
is unchanged: capture incidents use closed safe codes, stable attempt IDs and
`work_preserved: false` until explicit evidence-based source recovery.

Stop only the selected observer, retain its private config, and prepare a new
config differing only in `state_dir` and the two capture fields. Run
`observer.py --config NEW --adopt-capture-from PRIOR` before selecting the new
config/release. This helper refuses a live observer lock or any other binding
change, verifies a SQLite backup, preserves revisions and source incident state,
and leaves the original state available for software rollback. Restart the
observer after adoption; neither migration nor rollback dispatches producer work.

`capture_supervisor.py --project --state-dir ... --binding-sha256 ...` reads only
the supervision ledger. A later successful hook does not automatically resolve
older gaps. After explicit source recapture, select the retained failure and
confirmed matching recovery via `--resolve-attempt ID --recovery-attempt ID
--state-dir ...`; the helper requires the same source/event/binding and later
successful capture. This records resolution without invoking processing. It
does not authorize paid retries, source-scope changes or native goal resumption.

A verified scope exclusion is different from recovered source bytes. Reinvoke
the selected capture handler under the unchanged binding; a matching later
`skipped` attempt must retain a closed exclusion reason. Then use
`--exclude-attempt ID --recovery-attempt ID --state-dir ...` explicitly. The same
source, event and binding and later observation are required. Legacy skipped
receipts without an exclusion reason are insufficient. The resolved failure
retains its original reason, recovery reference and `source_excluded` resolution
kind. Its projection states that the source was excluded and keeps
`work_preserved: false`; no inbox, canonical record, paid dispatch or native goal
is created. Deploy the updated projection reader before issuing exclusions so
an older observer cannot mistake them for captured bytes. Runtime rollback must
retain a projection reader capable of rendering existing resolution kinds.

Attempts killed before source/event identity was recorded cannot be matched to
later successful or skipped operations. Keep those incidents open unless
independent operation evidence establishes their identity; this command supplies
no generic dismissal or timing-based guess.

Default behavior remains continuous user-service observation. Changing mode or
manager changes the retained observation binding: select a new private observer
state directory and reconcile any prior observation incident explicitly. Preserve
existing journals, revisions and delivery receipts. No HTTP contract change is
needed for finite-worker support.

Check `systemctl --user status daimon-alerts.service daimon-alerts-observer.service`,
then authenticated GET /v1/status and private observer `status.json`. The observer
is independently supervised, has no dependency that stops it with Alerts, and
records `service_state: unavailable` when HTTP is lost. Receiving adapter likewise
persists independent service and notification-route state. If both observers stop,
a separate receiving health consumer must expose its stale heartbeat; never use
the unavailable service to certify its own health.

## Backup and rollback

`daimon-alerts backup PRIVATE_CONFIG NEW_DESTINATION` uses online SQLite backup and
`PRAGMA integrity_check`. It refuses an existing destination. Retain checksums and
bounded rolling snapshots (operator policy, e.g. seven daily copies) outside the
service state directory; permission/access follows the existing backup route.
Backup restores must not overwrite later live receipts: stop only the Alerts
service, preserve current DB/WAL/SHM, verify the selected snapshot, restore with
owner-only permissions, start, and inspect known incidents/uncertain attempts.
Reconcile later external sends before any action. Producer journals stay intact.

Code rollback selects the previous pinned release and restarts only Alerts and/or
its observer. Never restore old state merely to roll software back. Migration
v1 is transactional; a future unsupported version refuses startup. Take an online
verified snapshot before future migrations. Preserve observer projection revision
state and receipt/reconciliation evidence on software rollback.

Retention prunes old resolved incidents only if no pending/uncertain delivery
remains. It runs on new-incident admission and bounds retained incidents. Live work
is never evicted; admission cap returns 503. Acknowledgments are capped at 100 per
incident; observers at 100. Delivery intents are bounded by incident transitions
and <=100 configured routes. Inspect capacity rather than silently increasing it.
Fictional testing and idle resource measurements do not prove production delivery.


Capture identity atomicity: for a valid selected native envelope, the first
attempt already contains its source hash and event. Killing the wrapper after
that first durable write preserves enough identity for later matching recovery;
there is no empty-attempt → identity-update interval. A missing or unclosed input
pipe times out visibly, without calling the capture handler. This does not claim
observation before Python starts or before the first durable write, and cannot
retroactively identify legacy attempts that lack metadata. The bounded input
failure retains `input_timeout`; deploy the compatible observer reader with the
wrapper. Source text and raw event payload remain absent from the attempt ledger.
