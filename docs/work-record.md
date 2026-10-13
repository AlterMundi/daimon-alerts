# Implementation work record

2026-10-13: Human requested end-to-end implementation and quality qualification.
Requirement: https://github.com/compaii/shared-resources/blob/main/docs/requirements/daimon-alerts.md
Receiving owner: https://github.com/AlterMundi/telecodex/issues/82
Producer owner: https://github.com/nicoechaniz/hermes-memory-kit/issues/113

Observed authenticated AlterMundi repository creation succeeded. Existing HMK
provider_status.py provides safe problem classification and a durable outbox.
Concurrent portable_producer.py and memory_worker.py work is preserved. Existing
Telecodex has concurrent documentation/example changes; production listener is
independently owned and must remain running.

Implementation sequence: contract/fixtures publication, durable core qualification,
safe producer observer, operations qualification, receiving adapter coordination,
real complete-loop receipts, resource and portable skill adoption.

No runtime, Telegram receipt or receiving adoption is claimed at this stage.

Contract bundle qualification: five real process/HTTP tests pass, with every
HTTP success/error validated against the published JSON Schema. Includes two
independent service processes competing for one SQLite delivery (32 claims,
exactly one authorization), SIGKILL/restart retaining uncertainty, actor-scoped
acknowledgment replay, immutable final receipt, revisions/resolution ordering,
permission denial, body limits, pagination, admission cap, online backup integrity
and source/observer stale status. `cargo clippy --locked --all-targets -- -D warnings`
passes. Runtime is local candidate software, not production delivery.

Safe producer projection published on HMK candidate PR114 at
54784326ae2091a7d427b64b029274115dc2f041. Thirteen producer/classifier/projection
checks pass; concurrent portable_producer/memory_worker changes remain untouched.
Observer HTTP qualification also passes with source preservation, resolution,
stall/unreadable source and independent outage status. Current core CI passes at
https://github.com/AlterMundi/daimon-alerts/actions/runs/38379396818 .

Receiving candidate in Telecodex's isolated concurrent-writer worktree preserves
its ordinary checkout. Seventeen existing dashboard HTTP/auth/control checks and
four new real core HTTP/SQLite adapter journeys pass. Sender is mocked in these
checks; no production receiving effect is implied. Existing UI bootstrap checks
pass. Delivery receipt loss reconciles without another send, uncertain dispatch
stays uncertain, and authenticated acknowledgment has independent CSRF/idempotency.

Release build and safe observer fault qualification pass. Added a16-request
admission bound and5-second body deadline; rechecked affected HTTP/process tests.
Python adapters use maintained HMK/Telecodex APIs; core runtime remains Rust.
No global Stop has been requested or invoked by this implementation.

Installed core and independent observer at741cfae. Exact executableSHA256:
23564f536bf5c26babb5bd2e8f5b3afeec0f245c9cb8d01be4c5a960359d5120.
Authenticated live HTTP confirmed one fictional HMK open incident and one pending
intent. Source work remains preserved and unprocessed (0 chapters). Idle cgroup
snapshot: core1490944bytes/8tasks/29msCPU; observer14376960bytes/1task/168msCPU.
Both units have128MiB limits. The production HMK binding is not adopted.

Resource service-daimon-alerts is registered in Shared Resources and the local
receiving map, with private config/grant references only in the ignored overlay.
Real Telegram/Sessions receipts and other-embodiment adoption remain unqualified.

Concurrent Telecodex receiving writer replaced the draft Alerts API with
Client/Sender/Receiver. Further receiver edits stopped, preserving both its work
and the draft surrounding integration. The mismatch and exact core reference are
recorded in #82. Earlier mocked-sender tests do not qualify the changed receiver.
Source UI inspected in Chromium at390px; fictional alert state/readability passed,
but this does not establish Telegram WebView or current receiving adapter behavior.
