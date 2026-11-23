# Native runtime and cgroup health

Use the existing `scripts/service_monitor.py` durable producer and the receiving
Alerts core/Telegram/Sessions adapter. No model, bot consumer, Matrix inbox or
failed-input replay is part of observation or infrastructure recovery.

Add only explicitly selected targets to an owner-private observer config:

- `kind: native_codex`, absolute `binary`, exact systemd `unit`, and `scope`
  (`system` by default, or `user`). A bounded native `app-server daemon version`
  control RPC must return a running server/version. An active unit alone is not
  readiness. Native restart-counter increases are independently retained.
- `kind: cgroup_memory`, absolute `cgroup_root`, and positive
  `full_avg60_percent` at most100. The probe observes actual sustained full PSI
  and OOM-kill counter increases. A nearly full reclaimable cache without stalls
  does not establish exhaustion. Initial lifetime counters establish a baseline,
  not a fabricated recent failure. Missing observation stays actionable.
- `kind: http_health` with an explicit loopback `base_url` checks the existing
  `/health` endpoint through a configured remote-service forward. Only bounded
  JSON with boolean `ok: true` proves availability. The route must actually reach
  the receiving service; an active forwarding unit alone proves nothing. Failure
  reports route/service unavailability without guessing whether the host, VPN or
  application caused it. This is external endpoint coverage, separate from the
  receiving host's native runtime/memory observations and provider acceptance.
- `kind: provider_auth`, with a fixed operator-owned `command`, accepts only
  `dm.monitor.provider-auth/v1` and the closed states `healthy`,
  `provider_login_required`, or `provider_unavailable`. The command is bounded
  to24seconds/4KiB; error text, tokens and account details never enter Alerts.
  `scripts/children_provider_health.py` is the existing four-guest host adapter:
  it uses each guest's selected native proxy and selected activity transport,
  reads `account/read` and authenticated `account/rateLimits/read`, and never
  starts a model or resubmits a conversation. Normal native token renewal remains
  with Codex. A cached account label or public `/health` success is insufficient.
  Install it through the current host operator route, then extend the existing
  services observer using `--extend-to`; preserve its journal, scope and timer.
  Its one private replaceable observation caches four concurrent checks for
  at most300seconds. `--fresh` verifies explicit login recovery immediately.
  Preserve the selected account; device login requires its account holder when
  the native refresh has been rejected. Retain that actual required action,
  rather than repeatedly attempting refresh or copying a revoked credential.
  Authentication success does not establish available quota, a useful model
  response, voice access or end-to-end conversation acceptance. The same incident
  resolves only after the authenticated check succeeds for all four guests.

Run the finite observer as the same receiving service owner with its actual HOME
and CODEX_HOME. Use a separately scoped publish/observe/status actor and existing
receiving route; keep tokens/configuration private. Select a timer interval below
the receiving core's configured freshness window, allowing for probe duration
and scheduling jitter: a30-second freshness window needs a15-second interval,
not a60-second interval that becomes stale between healthy invocations. A bounded
oneshot deadline selects technical observation only. Its persisted journal retains
incident/revision/outbox and counter baselines across restarts. Counter edges are
consumed only after the incident is durable; an unavailable service cannot erase
an already-baselined OOM or restart. A later healthy native/control observation resolves its incident. Memory
recovery requires full PSI below half the opening threshold for at least120seconds
of observed samples with no gap over90seconds. Threshold oscillation keeps the
same incident open; unknown samples reset recovery. The recovery window survives
observer restarts in the existing journal. Raw probe status remains separate from
`recovery_pending`; no incident or delivery history is cleared. Preserve historical
delivery and separate acknowledgment. Alert acceptance, Telegram acceptance and human reading differ.

The maintained Cluster native supervisor owns automatic recovery of an actually
departed daemon through the native idempotent Start. A timeout never establishes
process death and must not exit the cgroup supervisor, run ExecStop or kill work.
Persistent unresponsiveness alerts while the process is retained. No competing
restart loop, unbounded memory growth, account replacement or task replay is added.
Memory capacity changes are explicit operator selections against host headroom;
never claim that monitoring automatically creates RAM or solves every disk stall.

If the guest can stall, observe its selected host cgroup independently through the
host's existing Alerts route. This reads resource metrics without guest credentials
or model turns. Host memory observation does not establish guest native readiness
or disk health; retain the guest's actual control probe and its separate receiving
delivery evidence. Qualify the real incident and Telegram acceptance rather than
repeating a fictional outage. The same incident resolves through a later healthy
observation; acknowledgment does not clear it.

Install immutable selected source/client files and a separate scoped observer
state. Adding its actor requires only the receiving Alerts core to reload its
configuration; preserve its database and all existing actors/routes. Rolling
software back preserves the newer journal, incident and delivery history. Retire
an intentionally removed observer through the existing Alerts retirement control.

Qualification covers native response/timeout without starting a model, cache vs
real PSI, restart-edge persistence and service-unavailable replay. The HTTP fixture
waits for actual core readiness under one bounded30-second deadline and terminates
its own fixture on failure; it does not impose an accidental3-second cold-disk
performance requirement. CI scopes this adapter-only change to those contracts
and the actual core needed by the HTTP fixture; unknown/core changes keep full CI.


## Hosted enrollment registries

A host operator may configure one `kind: enrollment_registry` target with an
absolute private `registry`, fixed absolute `incus_binary`, ordinary id/resource
fields and positive `full_avg60_percent`. The observing UID owns the directory
and private regular JSON entries. Closed `cluster-runtime-health-enrollment/v1`
records contain only schema, name, instance, resource_ref and plan_digest.
Instance is exactly `dm-<name>` and resource is `container-dm-<name>`; no command,
home or credential comes from a record. The authorized Cluster job worker writes
them after Telegram completion.

New entries add resources without rebinding the journal. The writer atomically
limits the registry to16entries. A `.failure-<name>` marker makes enrollment
failure a durable incident without undoing accepted onboarding or prior coverage.
Cgroup results publish before remote RPCs. At most four native probes run
concurrently with separate15second restart-counter/version deadlines. Finished
incidents flush immediately; pending batches send ten-second unreadable heartbeats
until final observation. A full slow batch can take120seconds: use a180second
service bound and core freshness at least25seconds. This is native/resource
observation, not Matrix delivery or model availability.

See Cluster's [hosted runtime runbook](https://github.com/nicoechaniz/daimon-cluster/blob/main/docs/operations/hosted-runtime-health.md)
and [installer](https://github.com/nicoechaniz/daimon-cluster/blob/main/tools/install_host_runtime_health.py).
The host keeps its own receiving route, and copies no guest credentials.

## Relocating one hosted body

After verifying the receiving host's actual native/runtime and memory observation,
retire the old container enrollment with the same configured service monitor:
`service_monitor.py --config PRIVATE_CONFIG --retire-enrollment NAME`.
It acquires the existing monitor/registry locks, archives only `NAME.json` outside
the active registry glob, and resolves only that container's retained runtime and
memory observations with an explicit retirement summary. Other bodies continue to
be observed. A busy lock requires waiting for that finite observation to complete;
do not stop shared monitoring or a hosted runtime to obtain the lock.

The same command resumes a partial retirement. Pending publications replay exactly;
incident IDs, provenance, history, delivery receipts and source work are retained.
Retirement does not certify the new host healthy or resolve capture/provider work.
Keep native receiving observation and any independent external coverage distinct.
Rollback retains the archived registry record; restore it only after explicitly
returning the body to that container, with no duplicate active signed identity.

### Move a retained local observer to a different physical boundary

A VM is not a container cgroup. Inspect the actual receiving files as the service
user: a full-OS VM root often exposes PSI but no `memory.current`, `memory.max`
or `memory.events`. For system services, its real `system.slice` can provide the
native-work cgroup observation; it does not cover work in other slices or replace
independent parent-host/whole-VM monitoring. Keep that coverage distinction explicit.
For a full Linux OS, use `kind: host_memory` with the same bounded
`full_avg60_percent`. It reads global `/proc/pressure/memory` and `/proc/vmstat`
OOM-kill counters, validating `/proc/meminfo` availability. It covers all guest
processes/slices; a low available-memory value alone does not open an incident.
The same durable OOM edges and120-second sustained-low-PSI recovery apply.

After fencing the old observer/writers and transferring their coherent journal,
prepare a private receiving config with the actual targets and run the selected
monitor as its existing owner:
`service_monitor.py --config OLD_PRIVATE_CONFIG --relocate-to NEW_PRIVATE_CONFIG`.
Keep both files until adoption/rollback is settled. This operation permits target
location changes only: producer, observer, grants/token reference, scope, service
endpoint and state directory remain unchanged, as do target IDs. Probe kinds stay
fixed except an explicit `cgroup_memory`/`host_memory` boundary transition.
Hosted registries use their separate enrollment retirement control above.

The command first flushes exact retained publications, resolves only changed
targets' old observations with an explicit retirement summary, and waits for their
acceptance. It then atomically records the placement transition and resets only
those targets' counter/recovery baselines. Prior incidents and original counters
remain attributable; retirement never claims the receiving resource is healthy or
source work recovered. Ordinary starts still refuse an unexplained binding change.
Do not delete/recreate the journal to bypass that check. Retrying the same two
configs after a lost reply observes the retained transition without a new effect.

Select the receiving config for normal observation only after that operation;
verify an actual native/control and memory sample. A rollback performs the reverse
explicit transition after fencing the newer writer, preserving newer journals and
accepted revisions. The local audit retains the latest32 placement/counter records;
it never deletes incident projections or pending publications. Alerts core history
and its independent configured retention remain unchanged.

### Independent availability after relocation

The same private operator registry also accepts `remote-NAME.json` with closed
schema `cluster-runtime-health-remote/v1`, `name`, `resource_ref`, retained
`plan_digest` and loopback `base_url`. It adds `NAME-availability` HTTP health
observation through the existing selected forward without changing observer
authority or journal binding. Verify that the forward actually reaches the new
body before selecting the record. This requires no guest credential and does not
revive an archived local-container enrollment. Endpoint availability is distinct
from the body's local native/global-memory monitor and provider acceptance.
Non-loopback URLs and extra executable fields are rejected. An open incident
cannot be rebound to another resource; retire its original observation explicitly.
Use `--config PRIVATE_CONFIG --retire-remote NAME` for that retirement. It archives
only the selected remote record, flushes exact pending publications and resolves
its existing availability incident without certifying a replacement healthy.


### Matrix service availability

An active Matrix unit does not establish that the group application has selected
readers. For a Matrix `unit` target, optionally set `receiving_command` to an
operator-owned argv (absolute executable, at most8 arguments). The existing
service monitor first checks the unit, then runs this finite adapter with a
12second deadline. It requires the closed `dm.monitor.matrix-receiving/v1`
response with a Boolean `ready`; false reports `matrix_reader_unavailable`.
Unknown, malformed or failed observations cannot resolve a retained incident.
No new scheduler, model activation, message or automatic retransmission is added.

The supplied `matrix_receiving_health.py --config PRIVATE_CONFIG` uses the body's
existing SDK and owner status grant. Its configuration contains `operator`
(the maintained Cluster operator import directory), `root`, the actual group
application `socket`, owner-private `state`, and `required_readers` pairs of
being/embodiment coordinates. Invoke it through that body's selected SDK Python;
keep these local bindings out of public source. It authenticates the current
application, requires every protected reader and a reader for every distinct
being in the current signed audience, then checks current credential validity,
authority epochs, roster and own loaded application/software selection. It reads
no inbox and wakes no model. Local reader readiness does not prove peer process
availability, delivery, model attention or a human reading; retain those separately.
Install the same local observation on each selected living receiver.
Adding a new member must not invalidate a healthy extended vector merely because
the configured protected vector is older. Conversely, omitting the new distinct
being or a protected body still fails; an unselected sibling of an already covered
being does not require reviving an archived embodiment.

For a software-only replacement of an existing Matrix health adapter, use
`service_monitor.py --config CURRENT --refresh-reader-to REPLACEMENT`. This
extends the existing explicit target-reconciliation route: only
`receiving_command` on existing unit targets can differ, and the replacement
must pass its authenticated read first. The existing bounded reconciliation
history supports lost-reply inspection. Incidents, pending versions and counters
are unchanged; the next ordinary observation must resolve the same incident at
a higher revision. Do not use physical relocation/retirement to close a reader
incident merely while updating its diagnostic software.

The adapter retains one replaceable read-only status request plus its lock. Exact
status reads recompute live native state; no token per tick or report history is
created. Its atomic temporary file is replaced on process/profile change. The
existing Alerts journal/outbox and core retention remain authoritative. Reuse the
existing service timer and limits. To disable just this observation, explicitly
reconcile the target without `receiving_command`; do not pause native work or
messaging. A software update is complete only after current all-required reader
qualification and selection, using Cluster's existing SDK-successor route. A
service restart is not a substitute for that selection after software changes.

## Scheduled maintenance health

A `scheduled_job` target observes an explicitly selected system/user service and
its matching enabled timer. Configure `unit`, `timer`, `scope`,
`max_age_seconds` (60–172800) and `max_run_seconds` (10–3600). The two-day
ceiling allows a daily job plus explicit scheduling/detection grace without
loosening its runtime bound. Choose that grace from the actual timer jitter and
incident-response need; it does not change the timer or defer a known failed
exit. Healthy includes
a successful inactive oneshot within its cadence or a running invocation within
its runtime limit. Disabled timers, failed/missing runs, stale outcomes and
overlong invocations are unhealthy. Monotonic systemd timestamps are compared
with this boot's uptime; no job output or command arguments are published.
This observes exit status, not application-specific cleanup correctness.

For a dependent job invoked by an existing timer's service, explicitly select
`trigger_unit` as that timer's matching service. The observed `unit` can then
have its own name. Receiving observation requires the timer's actual `Triggers`
to include `trigger_unit`, and that loaded service's actual `Wants`/`Requires`
and `After` to include the observed job. Missing pull-in/order makes observation
unhealthy even after an earlier successful manual job. The existing freshness,
failed-exit and runtime checks still apply to the job itself. This supports a
maintained service chain without another timer; it creates no dependency or
schedule by itself. Without `trigger_unit`, the original matching-name contract
is unchanged. Add this field through the existing explicit target extension or
relocation control; editing a bound configuration alone remains refused.

To enroll another target, prepare a private replacement config preserving every
existing target, authority and journal, then invoke the selected monitor with
`--config OLD --extend-to NEW`. It drains retained exact publications before
atomically recording the binding extension; exact retries use that receipt.
Select NEW only after acknowledgment. History and pending work are preserved;
this does not install another timer or authorize automatic repair. The existing
16-target limit and bounded32extension receipts apply.


## Local Matrix capability and listener health

The existing Matrix receiving probe warns with `matrix_local_capability_expiring`
when its local status role has at most24hours remaining. Current authentication
and signed reader validity stay separate. This observation neither renews grants
nor treats an expired grant as authority. Use the existing native V8 explicit
owner renewal transaction, preserving keys, identity and all uncertain requests.

An optional private probe setting `listener: {"host":"127.0.0.1","port":8732}`
requires the authenticated daemon PID itself to own that listening socket. It
reads the existing Linux process/socket inventory, bounded to4096fds and256KiB;
it sends no request and reads no inbox. A running forwarding proxy cannot satisfy
this check. Absence reports `matrix_listener_unavailable`. The existing monitor
accepts those two closed v2 states and retains v1 readiness compatibility.
Existing timer, authentication, journals, retry semantics and retention apply.
A healthy listener still does not establish delivery of a particular message.

## Bounded HTTPS deployment group

An explicitly configured `kind: https_health_group` observes up to four fixed
public health endpoints from the receiving observer host. Its fields are exactly
`id`, `resource_ref`, `kind` and `endpoints`, plus optional boolean `enabled`; each endpoint has only an opaque
`id` and its complete HTTPS `url` ending in `/health`. Credentials, query strings,
fragments, duplicate endpoints and unbounded lists are refused. The total across
all such groups in one observer is at most four; the existing 16-target limit
is unchanged. Configuration is an owner selection, not a remotely supplied URL.

The group performs only the existing bounded JSON GET with TLS verification,
no redirect or proxy, no authentication cookies and no model/browser operation.
At most four reads run concurrently. Every endpoint must return a JSON object
with boolean `ok: true`; any missing, malformed or failed response degrades the
whole group. Only the closed health code is retained; response bodies, URLs and
raw network errors do not enter incidents. This detects endpoint/route/service
unavailability from outside the deployment. It does not diagnose physical host
power, prove Conversation authentication or assert a working provider/model.

Use the existing explicit target-extension control to add the selected group,
preserving producer authority, all original targets, journal, incidents and timer.
Qualify the real endpoints and the subsequent periodic observer before claiming
receiving adoption. Keep controlled fault tests separate from real outages;
never power off a host or interrupt a child's service to test this adapter.

To stop only the new group's future reads, select `enabled: false` through the
existing explicit `--relocate-to` configuration reconciliation, preserving all
target identities, other fields, authority and journal. It reports intentional
standby without a request; retirement of an existing outage is not recovery.
The same supported control re-enables the exact selected endpoints. Keep a
compatible monitor with the group parser: a pre-group binary/config is not a
valid rollback, and the relocation control does not remove fixed target IDs.
Do not edit the DB binding, drop unrelated monitoring or restore an old journal.
# Reuse the service observer in an authorized heartbeat

The existing finite `service_monitor.py --config PRIVATE_CONFIG
--heartbeat-report` writes `heartbeat/heartbeat-report.json` under its existing private state
directory after the ordinary observation. Select this option on the already
installed observation command; it adds no timer, model, network request,
acknowledgment or recovery operation. Existing journals and producer bindings are
unchanged. Removing the option stops report production; after deselecting the
consumer, its two fixed report/staging filenames and report directory can be retired without deleting
the journal or its uncertain outbox.

The report retains exact incident IDs/revisions, source times, resource references
and safe incident text. `acceptance` distinguishes a pending local publication
from a service-accepted revision. Resolution is a higher revision of the same
incident, never an acknowledgment or invented recovery. At most32 entries/64KiB
are projected, open incidents first; `more_incidents` explicitly signals omitted
entries. Use the existing authenticated paginated incident API for the remainder.
Neither truncation nor an unavailable observer establishes health.

An explicitly selected existing heartbeat report source can compare `/incidents`,
`/service_state` and `/more_incidents`, excluding the changing `/checked_at` clock
from activation hashes. Check the report's observation time and the authenticated
current incident before repair; this local projection is navigation, not fresh
service truth or execution authority. An unavailable or stale source is unknown.
This does not replace independent observer-freshness monitoring. Keep existing
heartbeat resource bounds and global/lane controls, and select only actual
human-authorized scoped repairs. Verify the producer's higher resolution revision
and the attributed general-conversation return separately from report publication.

For a constrained native reader, grant only the dedicated `heartbeat/` directory
read-only through its existing named filesystem profile. Keep observer configuration,
credentials and the parent journal outside that grant. A directory rule survives
atomic file replacement; an inode-only rule on one report generation may not.
Use the existing writable task work area for its bounded review cursor.


### Intermittent Matrix observation failures

The existing authenticated Matrix probe retains one private `probe-failure.json`
in its already configured probe state directory when an unexpected read fails.
It records only observation time, a fixed failure classification, the failed
stage, an allowlisted exception type/contract field and a recognized public refusal
code when available. It never records
exception text, credentials, paths, messages or a status payload. One fixed
atomic staging file and one current record keep storage below8KiB; normal reads
do not erase the last failure. The probe state owns their retention/removal.

Inspect that dated record when diagnosing `observation_unreadable`; a later
successful read alone does not explain or repair the earlier failure. This
metadata is an operator diagnostic, not ordinary UI text or a new readiness
state. Probe exit status, incident identity/history and the existing observer
schedule remain unchanged. The probe recognizes the native typed roster-unavailable response, which has no
`audience` field. That response is unavailable, not malformed JSON. Only a native
`reader_roster_unavailable` marked `retryable=true` permits one additional
read-only status request through the same client and original eight-second
absolute deadline. A persistent/invalid roster, changed process, bad authentication
or invalid reader remains unavailable; no check is weakened. The private record
retains that observed roster error, retryability and bounded-read outcome.
This adds no service restart, input replay, provider request or model attention.

## Finite Matrix owner repair from an existing Reports task

`scripts/owner_repair.py` is an optional fixed host command for the owning
DaimonApp Alerts action. It does not add an observer, schedule, queue, model or
inbox reader. Install it together with `service_monitor.py` and `client.py` from
one selected release. Keep the existing observer configuration, producer grant
and journal; no new producer or incident store is needed.

The private operator selection contains `enabled: true`, an existing `target_id`,
its exact Matrix `unit` and `scope`, and an absolute `hold_file`. Select only the
model-free Matrix service with its existing authenticated `receiving_command`,
never the Codex/harness service. A present hold file prevents action. Existing
systemd restrictions still apply. For a system unit invoked by a non-root owner,
the fixed start uses the existing noninteractive `sudo -n -- systemctl` route.
It installs no sudo/polkit grant and never prompts; missing access stays
unconfirmed for operator recovery. User units use their existing user manager. The App's fixed command is:

```text
python3 RELEASE/scripts/owner_repair.py --config PRIVATE_OBSERVER_CONFIG --selection PRIVATE_SELECTION
```

App supplies the bounded original `daimon-alerts-repair-input/v1` on stdin:
`incident.id`, `incident.revision` and `incident.resource_id`. These must match
the original current producer incident. No command, path, unit or credentials
come from model input. Only a retained `unit_inactive` incident whose selected
unit is actually failed with a failure result permits one `systemctl start`.
Intentional inactive/active services, changed incidents, disabled selection,
maintenance holds and other faults refuse before starting anything. Verification
or authorization failures require their owning source repair, not blind restarts.

The adapter shares the existing nonblocking observer lock. Before starting, it
commits its original digest and operation ID in the existing journal: one current
record per configured target, replaced only for a later distinct incident.
Duplicate attempts refuse. A timeout or lost reply retains that claim; call the
same fixed command with `--inspect-stdin` and the **exact original stdin** to
observe without starting again. The authenticated reader must be healthy before
the existing producer transition can resolve that same incident at a higher
revision. Only its ordered outbox is flushed; unrelated incidents remain with the
ordinary observer. Pending acceptance stays unconfirmed. No observation heartbeat
or unrelated incident is declared healthy by this operation.

A successful receipt uses `daimon-alerts-owner-repair/v1` and includes the exact
source digest, original incident/resource/revision, higher accepted resolution
revision and operation ID. Inspection returns `existing_resolution_verified`;
that does not prove the original start caused recovery. Process startup alone is
insufficient. The normal App process deadline applies; each systemd call is
bounded at three seconds and the existing authenticated probe retains its own
bound. A deadline can leave an inspectable operation, never permission to replay.

This adapter covers failed Matrix service recovery only. General owner repairs,
actual automatic native dispatch and the attributed general-conversation return
must still be qualified through the selected App Reports action. Controlled
fixtures must not be reported as production recovery or full fleet acceptance.

Focused qualification: `python3 tests/owner_repair_io.py` covers original claims,
unknown start/acceptance, stale selection, holds, intentional stops, authenticated
reader failure, lock exclusion and unrelated outbox preservation. On an authorized
Linux operator host with an existing user systemd manager and the existing built
Alerts fixture executable, `ALERTS_SYSTEMD_FIXTURE=1 python3 tests/owner_repair_systemd_io.py`
uses one disposable failed user service and isolated Alerts database. It verifies
actual start, readiness, same-incident HTTP resolution and original-input inspection,
then removes its fixture state and service. This fixture's reader checks its own
service marker; it does not establish a real Matrix authenticated-reader outcome.
No native model or production incident participates.

### Immediate fixed recovery, independent of model review

The existing finite observer accepts `--owner-repair-selection PRIVATE_JSON` to
run its already-qualified fixed Matrix recovery immediately after observing and
publishing an eligible incident. This reuses the same producer lock/journal,
selected target, maintenance hold and durable once-only claim. There is no second
scheduler or model turn. Only a genuinely failed selected service is eligible;
intentional inactivity, other targets and account-login failures are not starts.
An uncertain start is followed only by inspection of the exact original input;
a legacy claim lacking its original revision remains unconfirmed. Authenticated
receiving and accepted same-incident higher-revision resolution establish recovery.
The ordinary safe report carries that outcome for change-driven notification.
Do not require a Reports/LLM execution before invoking this programmatic recovery.

### Conversation public access

Add one `conversation_access` target to the existing critical-service observer
using its `--extend-to` operation. Select the owning DaimonApp
`scripts/interaction_access_probe.py` with an absolute Python executable,
an immutable absolute script path and up to eight exact Home HTTPS origins.
The group consumes a single bounded pass on the existing cadence. It checks
the public Conversation shell and expected anonymous API fence. A known
receiver/Alerts binding rejection opens an availability incident distinct from
transport or unexpected response. The next healthy full-roster observation
resolves it. Invalid, partial or inconsistent output is an observation failure.

The observer's existing tick receipt includes `targets[].receivers` for this
target when the full roster was observed. Each row contains only the configured
Home slug and the closed App observation state. Use those rows for the owning
Sysops handoff, including when one receiver has a binding failure and another
has a transport failure. Missing rows indicate unavailable observation. The
group keeps one incident and reports affected Home names without exposing
response bodies or adding another probe.

Inspect the App probe's per-origin result to locate an affected receiver.
Use the owning App requalification for a known binding change. Preserve
the current actor, dependent signatures and voice maintenance fence. A public
probe cannot establish a human's authenticated browser journey. No credentials
are accepted by the probe and this observer does not apply any App repair.

An exact healthy resolution from a configured authenticated unit probe includes
`resolution_evidence` on its existing heartbeat incident projection. It records
the authenticated read that closed that incident and its observation time.
Standby, retired placements and checks without an authenticated reader do not
carry this evidence. A successful observation cannot establish the cause of the
preceding failure or attribute a service restart without its own receipt.

For new authenticated reader incidents, the local observer keeps the opening
observation, successful authenticated read time, and same-incident owner repair
state in one bounded journal. Retention keeps 128 recent rows plus current
incidents. A successful start response, a claimed start with unknown outcome,
and no matching repair record remain distinct. Historical incidents without
an opening observation explicitly say it was not retained. An observation or
start acknowledgment does not attribute the underlying fault or causal repair.

The authenticated receiving probe spaces reads after typed, retryable roster
contention with finite backoff (100, 300, 700, 1200 ms). All reads still share
the original eight-second absolute client deadline and process binding.
Persistent contention, invalid roster, bad authentication and a changed
process still fail readiness. No service start is attempted for this result.

If an operator check overlaps a scheduled check, the probe waits up to 1.5
seconds for the shared status request lock. Persistent probe contention is
reported as probe contention, without claiming the Matrix reader failed.
An authenticated roster observation distinguishes transient database contention,
an unavailable roster and an invalid roster. The monitor retains those typed
opening observations alongside each incident's verified resolution and any
matching repair receipt.
