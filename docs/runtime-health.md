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
application, compares the required reader vector, current credential validity,
authority epochs, roster and own loaded application/software selection. It reads
no inbox and wakes no model. Local reader readiness does not prove peer process
availability, delivery, model attention or a human reading; retain those separately.
Install the same local observation on each selected living receiver.

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
