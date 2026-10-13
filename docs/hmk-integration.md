# HMK receiving integration

Daimon Alerts owns final safe worker observation and backend qualification.
HMK owns capture admission, the selected worker profile, model dispatch, journals
and explicit recovery. Telecodex owns Telegram and Sessions. This handoff replaces
the earlier assumption that observing a finite worker requires an always-active
daemon. It does not activate source capture or paid processing.

## What the HMK session supplies

Use the existing `memory_runtime.py`, `memory_supervision.py` and
`provider_status.safe_projection` interfaces. Supply the receiving operator with
private references to the selected worker profile and its installed HMK release,
the capture inbox and complete immutable binding, worker state directory, service
unit and manager (`user` or `system`), and authorized cadence/runtime/grace policy.
Identify whether this profile is prepared or actually activated. State its
selected source boundary; do not infer permission to ingest other conversations.
No source text, credential value or raw provider exception belongs in a handoff.

The canonical memory database is not an Alerts source. Alerts reads only the bound
capture journal's safe projection and `hmk.worker-status/v1`. No HMK HTTP server,
notice transport, processing queue or alternate recovery command is needed.
Do not pass service acceptance to HMK's notice sender as Telegram delivery.

## What Alerts configures

Resolve `service-daimon-alerts` through the receiving resource map. Configure a
private producer credential with `publish`, `observe`, `status` and the exact
producer/observer IDs and receiving scope. Browser and receiving adapter grants
remain separate. Preserve the existing service and receiver credentials.

The observer configuration documented in `operations.md` uses the supplied
`inbox`, complete `inbox_binding`, `worker_state_dir`, and pinned `hmk_source`.
Select `worker_mode: "oneshot"`, the actual `worker_unit`, and
`worker_systemd_scope`. Select a fresh private `state_dir` for a new production
binding; do not repurpose the fictional qualification observer's revision store.
Choose `source_freshness_seconds` from the authorized cadence plus expected
runtime/grace, and `stalled_seconds` from the selected phase runtime budget.
Observation runs independently of worker invocations and the Mini App.

For the authorized midnight UTC schedule, select `worker_daily_utc: "00:00"`
and an explicit runtime/grace allowance. The observer compares invocation start,
not merely receipt update time, against the expected day. Before the first worker
receipt it reports unobserved/not-started through the existing nonhealthy v1
projection, even when HMK has not created the operational outbox. It never
initializes that source state. A failed launcher before its first receipt remains
an actionable worker failure.

Exit 0 and fresh idle/pending state do not mean worker death. Exit 2 and pending
state preserve the processing incident while observation remains available.
Launch failure, signal death, stalled running receipts, overdue/unreadable state
and observer/service outage remain explicit. Systemd may discard successful
oneshot metadata; the fresh HMK receipt is still required. A timer alone proves
neither processing nor observation health.

## Complete-path qualification

Use an explicitly selected qualification source with the real receiving launcher
and provider/recovery interfaces. Controlled failure evidence must be identified
as controlled; prior mocked-provider fixtures are not live-worker acceptance.
Never spend money or retry an unknown paid dispatch just to fabricate a failure.

1. Record selected source/release, stable operation ID and pending work before
   the failure. Confirm HMK retains the batch and safe provider incident.
2. Observe durable Alerts acceptance and the same incident across repeat ticks
   and process restart. Record independent Telegram acceptance through Telecodex
   and Sessions state. Reading and acknowledgment require their own evidence.
3. Inspect the exact retained failed/uncertain request. Use HMK's existing explicit
   `memory_runtime.py retry --profile ... --request ... --reason ...` where
   required; it authorizes only that retained attempt and makes no network call.
   Invoke the selected finite worker explicitly after access is restored.
4. Verify source preservation, exactly-once committed work and retained paid
   dispatch receipts. A repeat invocation must replay completed work. Alerts
   publishes resolution for the same incident at a higher revision; repeated
   observations must not create another delivery intent or Telegram dispatch.

Retain source processing, service acceptance, delivery, human acknowledgment and
recovery receipts separately. Existing uncertainty must survive this integration.
Production adoption remains unqualified until the selected worker and receiving
effects are observed; software readiness does not substitute for those receipts.
