Implement the common durable incident service requested by Nicolás on 2026-10-13.

First publish `daimon-alerts/v1` contract, schemas and executable fictional fixtures
for [Telecodex #82](https://github.com/AlterMundi/telecodex/issues/82). Then implement
transactional ingress/read/acknowledgment/claim/receipt, independent safe HMK
observation, installation/backup/rollback and real receiving qualification.

Source requirement: https://github.com/compaii/shared-resources/blob/main/docs/requirements/daimon-alerts.md
Producer owner: https://github.com/nicoechaniz/hermes-memory-kit/issues/113

Acceptance requires real Telegram/Sessions receipts, preserved pending work and
explicit once-only HMK recovery, fault/concurrency/privacy tests, discoverability
and actual receiving skill/resource adoption. Mocked delivery is separate evidence.
Production Telecodex and concurrent HMK work must remain preserved.
