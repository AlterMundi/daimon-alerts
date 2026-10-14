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

HMK published its consolidated receiving handoff in issue1/comment6157385138:
forward-only capture is activated, midnight UTC processing is authorized, and
prepared finite units await Alerts observation before controlled invocation.
Resolved CompAII's real profile/inbox references through the private map and
verified the complete journal binding read-only. Its worker has not started and
its outbox is not initialized. Added explicit first-observation/launch-failure
handling without mutating that journal, plus selected daily-cadence detection
using invocation start rather than fresh status updates. Four targeted tests
passed: finite result handling, first-launch/missed-run policy, actual HTTP
unobserved projection with byte-identical source journal, and projection/recovery
and independent service faults. HTTP contract and consumer enums are unchanged.

CompAII's real bound-journal observer is now installed independently atcf34d2e,
using HMK103a5a0 and a new own producer/observer credential restricted to its
receiving scope. Preserved the core's existing actors/routes and receipts; online
verified backup preceded the additive grant and Alerts-only restart. Neither
production Telegram listener nor HMK worker/timer was started or restarted.
Observer active,15.5MiB/1task,128MiB bound; real source journal remained
byte-identical. Source status is explicitly unobserved with null source time:
no first worker receipt is invented. Supervised Telecodex1983a929 now has delivery
enabled by its operator and accepted this observation notice as`telegram-11066`.
Its installed Sessions backend reads the same real-source incident and delivered
receipt; phone reading/acknowledgment remain unobserved. No provider failure,
processing success or recovery is inferred from this initial coverage notice.

Private readiness receipt resolves through the existing HMK capability-map route
as`alerts_connection_ready`. CompAII is ready for HMK to install its prepared
finite service with timer disabled and perform the selected controlled invocation.
The complete failure/recovery/resolution loop remains pending. Eko/Oliva's actual
observers/grants/delivery routes remain unconnected here; do not inherit this
CompAII receipt. A hook failure before any inbox write has no configured durable
coverage signal in this integration and remains an explicit gap.

Human assigned integration and testing to this session, including receiving
worker operation. Installed CompAII's prepared finite service/timer with timer
disabled; verified live-memory and capture snapshots preceded first invocation.
Real installed HMK103a5a0 completed five steps with capture/organization/index
idle and earlier source revisions preserved. The production observer correctly
resolved its first-observation incident; Telegram accepted resolution11211.

Controlled failure uses the same installed finite runtime/model-receipt and
explicit-retry APIs with an isolated fictional corpus and deterministic provider
transport/embedding doubles. It is not an actual provider-credit outage or paid
dispatch. The reusable driver/test at4fc4403 retains a failed402 receipt and
requires the existing receipt-bound retry before completion. Failure/repeat,
explicit recovery, retained failure+success chain, one chapter and replay with no
new dispatch/embedding all passed in a separate temporary test destination.
Supervised receiving qualification is now held open for human phone inspection:
one selection/payment_required incident accepted as`telegram-11234`, visible in
the installed Sessions backend. Source is preserved,0chapters,1 failed controlled
dispatch and0 successful ones; repeated failure invocation reuses the receipt.
Phone observation/acknowledgment and receiving-path recovery/resolution remain
pending; an earlier stale fictional-observer incident remains separately visible.

Human phone acceptance: Nicolás reported the open selection/payment_required
notice with delivered state. Explicit receipt-bound controlled recovery then
completed1 chapter, preserved source and retained the failed attempt. Replay was
idle with0 new transport/embedding calls. Same incident resolved atrevision2;
real Telegram accepted resolution`telegram-11240` after open`telegram-11234`.
Installed Sessions reads that resolved projection and both receipts. A later
read observes1 authenticated acknowledgment following the human's stated click;
no producer recovery is inferred from that acknowledgment. This is a controlled
fictional transport failure through the real finite-worker APIs and supervised
receiving route, separate from the successful real bound-journal invocation.

Human requested clear attention and independent processing/acknowledgment states.
Telecodex PR91 merged at17dad033; its dashboard CI and UI regressions passed.
Installed UIcda6045 adds amber warning/unacknowledged count, explicit Open/Resolved
and Acknowledged/Unacknowledged badges, and collapsed resolved history that keeps
its expansion across refresh. Existing backend/helper bytes and configured grants
are retained. SQLite snapshots verified before optional Sessions sidecar restart;
native listener PID stayed unchanged. First package missed its existing activity
helper and was rolled back; actual traceback identified the missing scripts
module, helper was included, and package import/health/served-byte checks passed.
Final health200, anonymous Alerts401; phone rendering of the new layout remains
to be observed. Retention remains core-owned, with no browser deletion operation.

CompAII's already-authorized midnight UTC schedule is now enabled after its
worker/observer/receiving qualification; next invocation is2026-10-15 00:00 UTC.
No explicit paid retry was scheduled or authorized by an alert. Eko/Oliva's
receiving connections remain unqualified, and pre-inbox hook-failure coverage
remains an explicit gap. Do not mark the overall multi-receiver task complete.

After a human-reported native turn failure, inspected retained effects before
continuing: core, real observer, Sessions and the authorized timer remain active;
UI releasecda6045 and the controlled resolved/acknowledged receipt pair are intact.
No test dispatch or producer recovery was repeated. Read-only receiving preflight
through the existing sudo/Incus route confirms Eko/Oliva HMK handoffs are present,
but their current Telegram configs have no session_control grant and no Sessions
sidecar unit is installed. Their existing listeners remain untouched. Requested
the supported separate receiving grants/adapters/resource-access route from
Telecodex #82/comment6159775458. The CompAII loopback route is not silently reused
across beings; other-body adoption remains pending actual receiving configuration.
