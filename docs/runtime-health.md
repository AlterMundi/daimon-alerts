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

Run the finite observer as the same receiving service owner with its actual HOME
and CODEX_HOME. Use a separately scoped publish/observe/status actor and existing
receiving route; keep tokens/configuration private. Select a timer interval below
the receiving core's configured freshness window, allowing for probe duration
and scheduling jitter: a30-second freshness window needs a15-second interval,
not a60-second interval that becomes stale between healthy invocations. A bounded
oneshot deadline selects technical observation only. Its persisted journal retains
incident/revision/outbox and counter baselines across restarts. Counter edges are
consumed only after the incident is durable; an unavailable service cannot erase
an already-baselined OOM or restart. A later healthy native/control or memory
observation resolves the incident, preserving its historical delivery and separate
acknowledgment. Alert acceptance, Telegram acceptance and human reading differ.

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
