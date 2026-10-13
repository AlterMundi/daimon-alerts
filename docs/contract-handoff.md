Published consumer contract and executable fictional runtime:
**d52c72102adc66078dd458cf870b2e1ac5727b1b** in AlterMundi/daimon-alerts.

- `docs/consumer-contract.md`
- `schemas/daimon-alerts-v1.json`
- `fixtures/{open,resolved,success,denial,stale,conflict,unavailable,uncertain}.json`
- `scripts/fixture_server.py` (uses compiled Rust/SQLite core; generated private
  development credentials, isolated temporary state)
- `tests/http_contract.py` (five real process/HTTP tests; all responses checked
  against schema; two SQLite consumers, 32 claims, one dispatch authorization)

This contract can be consumed before production HMK or Alerts installation.
No real Telegram delivery or adoption is claimed. Processing, acknowledgment,
service acceptance and delivery remain independent. Repeated/ambiguous claims
never grant another send. Core work: https://github.com/AlterMundi/daimon-alerts/issues/1
