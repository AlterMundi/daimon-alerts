# Daimon Alerts

Durable operational incidents and safe receiving delivery, independent of models
and Mini Apps. HMK is the first producer; Telecodex owns Telegram and Sessions.

Start with [the consumer contract](docs/consumer-contract.md). Contract and runtime
qualification are separate; see [the work record](docs/work-record.md).

Rust + SQLite. Build: `cargo build --locked`. Test: `cargo test --locked`.
No listener, model, inbox polling or paid recovery starts on installation.
