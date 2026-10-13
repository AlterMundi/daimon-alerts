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

Real receiving qualification: existing authenticated bot accepted fictional open
notice `telegram-9625` and resolution notice `telegram-9651`, via Telecodex
candidate59467e3bf6d6c7f4a798b69cde7aef517774505f (PR86). Retained service intents
and receiving receipts remain separate from HMK's pending source-notice field.
Another receiver pass produced no additional dispatch. Candidate Provider's
actual operations projection shows resolved, but production Sessions is not yet
updated and no phone/client reading is claimed.

Explicit fictional HMK recovery produced one chapter; next invocation was idle,
with exact original source hash retained. Qualification failures are preserved:
concurrent producer edits first refused code drift; exact pending code hash
matched393ccf1848b48c8ee820364b9be8bde36ac2e473, extracted only for the necessary
pinned recovery. The driver then proposed an invalid title absent from its source;
HMK preserved attempts and halted. Corrected the driver to the literal `Fictional
Jo`, validated against frozen sources, saved the failed job and explicitly reopened
only that driver's known title-error halt. No code/binding guard was changed.
Fresh driver failure/recover/replay qualification passes without manual repair.
No real provider or live memory corpus was used.

Shared alerts skill merged in Skills PR57 atb6d0170f6a8ec42a9fb8aed6174060b839d4e7ad.
Selective describe/prepare/apply/status qualification and actual local installation
passed; neutral surface-check is ok and this native turn read the installed guide.
Other embodiment adoption is not established by this local installation.

Remaining goal work: production receiving-side integration/phone acceptance,
production HMK owning adoption, authorized other-embodiment discover/read/publish
qualification and resource/skill receiving receipts. Do not mark the goal complete
from the successful fictional loop alone. Existing production Telecodex listener
and dashboard have not been stopped/redeployed by this work.

Installed supervision fault receipts: stopping only the qualification observer
made authorized Status degraded/stale; stopping only Alerts left the independent
observer running and recording service unavailable. Restore returned healthy with
both Telegram receipts/intents unchanged. A separately supervised fictional worker
was killed with SIGKILL; the safe observer detected worker_stopped while its last
running receipt was still fresh. No existing production service or global Stop
was involved. Latest core CI0ae802d passed:
https://github.com/AlterMundi/daimon-alerts/actions/runs/38383548770 .

Telecodex PR86 now uses82a16d019eeb8aafdf0f542d2284a27dd5ad0315; only the new
Python test filename/CI declaration changed after the real receiving qualification.
Moved that test into the repository's existing dashboard CI scope to avoid an
unrelated full Rust suite. Actual receiving code remains the qualified59467e3 bytes.

Native goal continuation: prior turn made progress through deployed core, real
receipts, qualification and published integration/skill. Rechecked current sources:
Telecodex PR87 has passing receiving CI and records its operator-owned activation;
production Sessions still uses the original runtime. Its stronger receiving changes
remain owned there, rather than restarting that independently owned service here.

Qualified the advertised fixture command itself: it starts a real listener and
SIGTERM terminates its child without leaving a listener. Fixed startup to retain
one identical upsert payload across retries, preserving idempotency if acceptance
succeeds but its reply is lost. This test is now included in core CI. Refreshed the
maintained requirement's stale pre-implementation status with observed qualification
and explicit pending adoption, without changing the acceptance requirements.

Observed receiving activation by its operator: production Sessions now selects
1983a929632cb62ea260c96079989152553a45e3 (PR87), process active. Its actual config
has Alerts enabled, matching scope/adapter, and delivery disabled. Health returns
200 and anonymous /api/alerts returns401. The installed receiver's authenticated
operator read reports available/healthy and reads the resolved fictional incident
with both delivered receipts. This is actual installed read qualification, not a
phone/signed-browser journey or supervised-send acceptance. This task did not
restart/redeploy that service. Existing pending phone/adoption actions remain
unfulfilled; no duplicate request or fabricated acknowledgment is emitted.

Human assigned final HMK/backend integration to this session; Telegram/Sessions
remain with Telecodex. Inspected current merged HMK94d2186 interfaces and finite
supervision handoff. Added explicit oneshot/user-or-system observer selection,
preserving legacy default bindings and requiring separate state for changed
semantics. Successful finite completion and pending exit2 no longer appear as
daemon death; stale/running/crashed/failed-launch observations remain explicit.
Two targeted observer suites passed. Actual temporary user-systemd units verified
exit0/idle and exit2/pending as observable, and exit1 as worker_stopped. The first
real check exposed that systemd discards successful invocation metadata; corrected
the observer to require the fresh HMK receipt instead of an absent exit-code
record. Temporary units were removed. No real pool, capture hook, paid request,
production worker or existing receiver was changed. Published concrete HMK
connection and complete-path handoff in docs/hmk-integration.md; actual selected
production binding and full worker-path receiving receipts remain outstanding.
