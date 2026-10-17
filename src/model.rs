use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub const SCHEMA: &str = "daimon-alerts/v1";
pub const CODES: &[&str] = &[
    "payment_required",
    "quota_exhausted",
    "authentication_failed",
    "access_denied",
    "rate_limited",
    "service_unavailable",
    "timeout",
    "transport_error",
    "invalid_request",
    "response_incomplete",
    "dispatch_unresolved",
    "provider_error",
    "worker_stopped",
    "worker_stalled",
    "observation_stale",
    "observation_unreadable",
    "notification_unavailable",
    "storage_pressure",
];
pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
pub fn hash(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}
pub fn id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
pub fn plain(value: &str, max: usize) -> bool {
    !value.is_empty() && value.chars().count() <= max && !value.chars().any(char::is_control)
}

#[derive(Clone, Deserialize, Serialize, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
pub struct IncidentInput {
    pub schema: String,
    pub producer_id: String,
    pub incident_id: String,
    pub operation_id: String,
    pub revision: i64,
    pub receiving_scope: String,
    pub phase: String,
    pub resource_ref: String,
    pub severity: String,
    pub problem_code: String,
    pub summary: String,
    pub action: String,
    pub source_at: i64,
    pub updated_at: i64,
    pub processing_state: String,
    pub work_preserved: bool,
}
impl IncidentInput {
    pub fn valid(&self) -> bool {
        self.schema == SCHEMA
            && self.revision > 0
            && [
                &self.producer_id,
                &self.incident_id,
                &self.operation_id,
                &self.receiving_scope,
                &self.phase,
                &self.resource_ref,
            ]
            .into_iter()
            .all(|s| id(s))
            && ["info", "warning", "error", "critical"].contains(&self.severity.as_str())
            && CODES.contains(&self.problem_code.as_str())
            && ["open", "resolved"].contains(&self.processing_state.as_str())
            && plain(&self.summary, 280)
            && plain(&self.action, 500)
            && self.source_at >= 0
            && self.updated_at >= self.source_at
            && self.updated_at <= now() + 60
    }
    pub fn same_provenance(&self, other: &Self) -> bool {
        self.producer_id == other.producer_id
            && self.incident_id == other.incident_id
            && self.operation_id == other.operation_id
            && self.receiving_scope == other.receiving_scope
            && self.source_at == other.source_at
            && self.phase == other.phase
            && self.resource_ref == other.resource_ref
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actor {
    pub actor_id: String,
    pub token_sha256: String,
    pub producers: Vec<String>,
    pub scopes: Vec<String>,
    pub adapters: Vec<String>,
    pub grants: Vec<String>,
}
impl Actor {
    pub fn grant(&self, grant: &str) -> bool {
        self.grants.iter().any(|v| v == grant)
    }
    pub fn scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|v| v == scope)
    }
    pub fn producer(&self, producer: &str) -> bool {
        self.producers.iter().any(|v| v == producer)
    }
    pub fn adapter(&self, adapter: &str) -> bool {
        self.adapters.iter().any(|v| v == adapter)
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub receiving_scope: String,
    pub adapter_id: String,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub database: String,
    pub listen: String,
    pub freshness_seconds: i64,
    pub max_incidents: i64,
    pub retention_seconds: i64,
    pub actors: Vec<Actor>,
    pub routes: Vec<Route>,
}
impl Config {
    pub fn valid(&self) -> bool {
        self.freshness_seconds > 0
            && self.max_incidents > 0
            && self.max_incidents <= 100000
            && self.retention_seconds >= self.freshness_seconds
            && !self.database.is_empty()
            && self.actors.len() <= 1000
            && self.routes.len() <= 100
            && self.actors.iter().all(|a| {
                id(&a.actor_id)
                    && a.token_sha256.len() == 64
                    && a.token_sha256.bytes().all(|b| b.is_ascii_hexdigit())
                    && a.producers
                        .iter()
                        .chain(&a.scopes)
                        .chain(&a.adapters)
                        .all(|v| id(v))
                    && a.grants.iter().all(|v| {
                        [
                            "status",
                            "publish",
                            "read",
                            "acknowledge",
                            "deliver",
                            "observe",
                        ]
                        .contains(&v.as_str())
                    })
            })
            && self
                .routes
                .iter()
                .all(|r| id(&r.receiving_scope) && id(&r.adapter_id))
            && (0..self.actors.len()).all(|i| {
                self.actors[..i].iter().all(|a| {
                    a.actor_id != self.actors[i].actor_id
                        && a.token_sha256 != self.actors[i].token_sha256
                })
            })
            && (0..self.routes.len()).all(|i| {
                self.routes[..i].iter().all(|r| {
                    r.receiving_scope != self.routes[i].receiving_scope
                        || r.adapter_id != self.routes[i].adapter_id
                })
            })
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimInput {
    pub request_id: String,
}
#[derive(Clone, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReceiptInput {
    pub request_id: String,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_ref: Option<String>,
}
impl ReceiptInput {
    pub fn valid(&self) -> bool {
        id(&self.request_id)
            && ["uncertain", "delivered", "failed"].contains(&self.outcome.as_str())
            && self
                .message_ref
                .iter()
                .chain(self.destination_ref.iter())
                .all(|v| id(v))
            && (self.outcome != "delivered"
                || self.message_ref.is_some() && self.destination_ref.is_some())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationInput {
    pub receiving_scope: String,
    pub source_at: Option<i64>,
    pub source_state: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverRetirementInput {
    pub receiving_scope: String,
}
#[derive(Default, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Page {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub producer_id: Option<String>,
    pub processing_state: Option<String>,
    pub adapter_id: Option<String>,
    pub state: Option<String>,
}
