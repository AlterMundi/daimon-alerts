Core runtime and supervised safe HMK projection are published at
741cfaeb2e91b6f0486360787286a68ad1c27cae. CI passed:
https://github.com/AlterMundi/daimon-alerts/actions/runs/38381015225
HMK safe_projection is published at54784326ae2091a7d427b64b029274115dc2f041 in PR114.

I observed a concurrent replacement of the receiving adapter while testing the
Telecodex candidate. I am preserving that work and stopping edits to the receiver
until ownership is reconciled. My earlier four adapter journeys used an `Alerts`
class and were mocked-sender evidence; the current concurrent code instead exposes
Client/Sender/Receiver and cannot inherit those test results. Existing Provider
and my draft test still refer to `Alerts`; these need reconciliation before any
receiver deployment. No production Telecodex restart or real Telegram send has
been performed by Daimon Alerts work.

Core ingress/read/ack/claim/receipt semantics remain frozen in the published
contract. I will proceed with independent core installation and qualification.
Receiving readiness and real Telegram/Sessions acceptance remain separate.
