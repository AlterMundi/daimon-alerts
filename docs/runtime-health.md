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
