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
