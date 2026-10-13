# Daimon Alerts consumer contract

Contract: `daimon-alerts/v1`. All representations are UTF-8 JSON. The accompanying
JSON schemas freeze closed request/response objects. Unix times are integer
seconds UTC; producer revision is a positive 64-bit integer. The service has no
paid work, goal, model, provider-switch or processing-recovery endpoint.

## Authority and transport

Initially bind authenticated HTTP to loopback. A receiving backend supplies an
Authorization Bearer token from its private credential reference. Never put
credentials in URLs, payloads, browser storage, notices or logs. Local credentials
resolve actor, allowed producer IDs, receiving scope IDs, adapter IDs and operation
grants. Producer scope is enforced on writes; receiving scope on every operation.
Browser clients use the existing authenticated receiving backend. Remote exposure
requires the existing resource-access authority; this local service does not grant
remote access or replace Cluster fences. Configured identifiers are opaque labels,
not being identity proofs. Retain upstream authentication attribution in the private
binding. No client-supplied actor field is accepted.

Grants: `status`, `publish`, `read`, `acknowledge`, `deliver`. Each operation also
requires a matching scope. Delivery credentials need both `deliver` and adapter
association. Auth failures return 401; grant/scope failures 403. Unauthorized
single-object reads return 404 to conceal existence. Collection reads omit other
scopes. Receiving scope and incident provenance cannot change after acceptance.

## Routes

| Method and path | Input | Success |
| --- | --- | --- |
| GET /v1/status | none | Status (200) |
| PUT /v1/incidents/{producer_id}/{incident_id} | IncidentInput | Acceptance (201 first, 200 update/replay) |
| GET /v1/incidents | optional producer_id, processing_state, cursor, limit | IncidentPage (200) |
| GET /v1/incidents/{producer_id}/{incident_id} | none | Incident (200) |
| PUT /v1/incidents/{producer_id}/{incident_id}/acknowledgments/{request_id} | empty JSON object | Acknowledgment (201 first, 200 replay) |
| GET /v1/deliveries | optional adapter_id, state, cursor, limit | DeliveryPage (200) |
| POST /v1/deliveries/{delivery_id}/claim | ClaimInput | Claim (200) |
| PUT /v1/deliveries/{delivery_id}/receipt | ReceiptInput | Delivery (200) |

Identifiers match `[A-Za-z0-9_-]{1,128}`. Limits default to 50, maximum 100.
Cursor is an opaque positive row sequence; do not parse or reuse across routes.
Collections are ascending creation order, bounded and not snapshot-isolated.
Concurrent insertion appears on subsequent pages. Retention can remove old resolved
incidents; applications needing archival evidence keep their own safe receipt.
Incident filtering accepts `open` or `resolved`. Delivery filtering accepts `pending`,
`uncertain`, `delivered`, `failed`; absent filter includes all. Adapter filtering
must be within the authenticated adapter association.

Request bodies are limited to 16 KiB. Unknown fields and invalid shapes return 400,
oversized bodies 413, unsupported media types 415. Errors are safe Error objects;
no raw exceptions. Clients use bounded connection/request timeouts (recommended
2/5 seconds). Unavailable connection, timeout, invalid response or HTTP 503 never
means acceptance, health or permission. HTTP 500 is a safe internal-error response.

## Projection and version ordering

IncidentInput carries stable producer/incident/operation IDs, receiving scope,
phase, resource reference, severity, safe problem code, summary/action, source and
update times, processing state and `work_preserved`. Summary (1–280 chars), action
(1–500 chars) are plain text with no control characters. They are never commands.
Resource reference is an opaque catalog identifier, never a private path/endpoint.
Problem codes are a closed allowlist including provider, worker and observation
failures. No HMK source rows, raw provider responses, session history or credentials.
Session association is deliberately absent until a receiver can verify it.

Source times cannot be in the future (60-second clock allowance); updated time
must be at least source time. Service `accepted_at` and `observed_at` report service
acceptance, not external worker liveness. `freshness` evaluates accepted observation
age against configured freshness_seconds; stale never clears producer state.
Producer revisions increase monotonically and only source changes increase them.
Same revision and canonical JSON content returns the original acceptance receipt.
Lower revision returns 409 `stale_revision`; different same-revision content returns
409 `revision_conflict`. Provenance/operation/scope/source time are immutable.
Resolution cannot reopen under the same incident ID. A recurrence uses a new
incident ID (e.g. new producer dispatch ID), retaining the stable operation ID.
Late open updates cannot overwrite resolution. Every acceptance is durable before
response. A lost upsert response can safely be retried with identical content.

## Delivery and acknowledgment are independent

The service creates one intent per incident state transition and configured
scope/adapter, transactionally with acceptance. Repeated observations or revision
changes within the same processing state create no new notices. Open and resolved
notices have distinct IDs. Receivers render the intent's captured projection so
later revisions cannot turn a queued open notice into an unlabelled resolution.

ClaimInput has `request_id` only. The atomic first claim persists state `uncertain`
and attempt ID BEFORE returning `dispatch_allowed: true`. Every later claim,
including the same request ID, returns `dispatch_allowed: false`. A lost claim
response therefore sacrifices dispatch availability to prevent a duplicate send.
Claim IDs are scoped to a delivery; the first adapter actor owns the attempt.
No lease expiration, restart, read or reconnect grants a second send.

ReceiptInput contains `request_id`, outcome (`delivered`, `uncertain`, `failed`),
optional safe opaque `message_ref` and `destination_ref`. `delivered` requires both
references and represents external API acceptance, never human reading. Only the
claim's authenticated actor/adapter can record its receipt. Exact receipt replay
is idempotent. An uncertain receipt can be reconciled to delivered/failed; a final
receipt cannot change. Failed means definitively not accepted but never authorizes
a retry. A new recovery protocol would require a separately versioned operation.

Acknowledgment is keyed by authenticated actor and request_id under the incident.
Replay returns its original time, including after resolution. Another actor's same
request_id is independent. Acknowledgment does not assert human reading, delivery,
processing completion or recovery. Incident responses retain independent arrays
of acknowledgments and deliveries; processing remains producer authoritative.

## Health and persistence

Status reports service time, observer records and freshness. No observer means
`unobserved`; a stopped observer becomes `stale`. Each observer observation includes
source state and source time, distinguishing stale/unreadable source from stale
observer. An unavailable service cannot certify itself: independent receiving
supervision must detect connection failure. Production observation uses the safe
HMK boundary and never dispatches source work. Observation ingress is operator
only and documented separately; it is not a browser operation.

SQLite transactions, full synchronous commits and migrations preserve acceptance,
acknowledgment and uncertain attempts across process death. Backups use SQLite's
online backup API and integrity verification. Retention never purges open incidents
or pending/uncertain deliveries; admission fails with 503 `capacity_exceeded` when
configured limits are reached, rather than discarding live work. Receivers must
show failures explicitly. Installation is separate from real receiving acceptance.

## Executable fictional development

`python3 scripts/fixture_server.py --port 8791` starts the compiled service with
an isolated temporary database and privately generated development credentials.
The process prints only the directory reference. Source `client.env` there to
load credential references; no credentials are printed. `fixtures/` contains
fictional open/resolved projections and success/denial/stale/conflict/unavailable/
uncertain examples. These are protocol examples, not production delivery evidence.
Run `python3 tests/http_contract.py` for executable HTTP/process acceptance.
