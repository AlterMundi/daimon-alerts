use crate::model::*;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{path::Path, sync::Mutex};

#[derive(Debug)]
pub struct Error(pub u16, pub &'static str);
pub type Result<T> = std::result::Result<T, Error>;
impl From<rusqlite::Error> for Error {
    fn from(_: rusqlite::Error) -> Self {
        Self(500, "internal_error")
    }
}
impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Self(400, "invalid_request")
    }
}
fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(data)?)
}
fn require(test: bool, code: &'static str) -> Result<()> {
    if test { Ok(()) } else { Err(Error(403, code)) }
}
fn parse_stored<T: DeserializeOwned>(data: &str) -> Result<T> {
    serde_json::from_str(data).map_err(|_| Error(500, "internal_error"))
}
pub struct Store {
    pub config: Config,
    pub connection: Mutex<Connection>,
}

impl Store {
    pub fn open(config: Config) -> Result<Self> {
        if !config.valid() {
            return Err(Error(400, "invalid_request"));
        }
        let mut con = Connection::open(&config.database)?;
        con.busy_timeout(std::time::Duration::from_secs(2))?;
        con.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        let version: i64 = con.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > 1 {
            return Err(Error(500, "internal_error"));
        }
        if version == 0 {
            let tx = con.transaction()?;
            tx.execute_batch(include_str!("../schemas/001.sql"))?;
            tx.commit()?;
        }
        Ok(Self {
            config,
            connection: Mutex::new(con),
        })
    }
    pub fn authenticate(&self, token: &str) -> Result<Actor> {
        if token.len() < 20 || token.len() > 1024 {
            return Err(Error(401, "unauthorized"));
        }
        let digest = hash(token.as_bytes());
        self.config
            .actors
            .iter()
            .find(|a| a.token_sha256 == digest)
            .cloned()
            .ok_or(Error(401, "unauthorized"))
    }
    pub fn handle(
        &self,
        actor: &Actor,
        method: &str,
        path: &str,
        page: Page,
        body: &[u8],
    ) -> Result<(u16, Value)> {
        let parts: Vec<_> = path.trim_start_matches('/').split('/').collect();
        if parts.iter().any(|p| !id(p)) {
            return Err(Error(404, "not_found"));
        }
        let mut con = self
            .connection
            .lock()
            .map_err(|_| Error(500, "internal_error"))?;
        let tx = con.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let result = match (method, parts.as_slice()) {
            ("GET", ["v1", "status"]) => self.status(&tx, actor),
            ("PUT", ["v1", "incidents", producer, incident]) => {
                self.upsert(&tx, actor, producer, incident, decode(body)?)
            }
            ("GET", ["v1", "incidents"]) => self.incidents(&tx, actor, page),
            ("GET", ["v1", "incidents", producer, incident]) => {
                require(actor.grant("read"), "forbidden")?;
                Ok((200, self.incident(&tx, actor, producer, incident)?))
            }
            (
                "PUT",
                [
                    "v1",
                    "incidents",
                    producer,
                    incident,
                    "acknowledgments",
                    request,
                ],
            ) => {
                let empty: Value = decode(body)?;
                if empty != json!({}) {
                    return Err(Error(400, "invalid_request"));
                }
                self.ack(&tx, actor, producer, incident, request)
            }
            ("GET", ["v1", "deliveries"]) => self.deliveries(&tx, actor, page),
            ("POST", ["v1", "deliveries", delivery, "claim"]) => {
                self.claim(&tx, actor, delivery, decode(body)?)
            }
            ("PUT", ["v1", "deliveries", delivery, "receipt"]) => {
                self.receipt(&tx, actor, delivery, decode(body)?)
            }
            ("PUT", ["v1", "observers", observer]) => {
                self.observe(&tx, actor, observer, decode(body)?)
            }
            ("POST", ["v1", "observers", observer, "retire"]) => {
                self.retire_observer(&tx, actor, observer, decode(body)?)
            }
            _ => Err(Error(404, "not_found")),
        }?;
        tx.commit()?;
        Ok(result)
    }
    fn status(&self, tx: &Transaction<'_>, actor: &Actor) -> Result<(u16, Value)> {
        require(actor.grant("status"), "forbidden")?;
        let mut statement = tx.prepare(
            "SELECT id, scope, observed_at, source_at, source_state FROM observers ORDER BY id",
        )?;
        let rows = statement.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, String>(4)?,
            ))
        })?;
        let mut observers = Vec::new();
        let mut degraded = false;
        for row in rows {
            let (id, scope, observed, source, state) = row?;
            if !actor.scope(&scope) {
                continue;
            }
            let fresh = now() - observed <= self.config.freshness_seconds;
            degraded |= !fresh || state != "healthy";
            observers.push(json!({"observer_id":id,"receiving_scope":scope,"observed_at":observed,"source_at":source,"source_state":state,"freshness":if fresh {"fresh"} else {"stale"}}));
        }
        Ok((
            200,
            json!({"schema":SCHEMA,"service_time":now(),"state":if observers.is_empty() {"unobserved"} else if degraded {"degraded"} else {"healthy"},"observers":observers}),
        ))
    }
    fn upsert(
        &self,
        tx: &Transaction<'_>,
        actor: &Actor,
        producer: &str,
        incident: &str,
        value: IncidentInput,
    ) -> Result<(u16, Value)> {
        require(
            actor.grant("publish")
                && actor.producer(producer)
                && actor.scope(&value.receiving_scope),
            "forbidden",
        )?;
        if !value.valid() || value.producer_id != producer || value.incident_id != incident {
            return Err(Error(400, "invalid_request"));
        }
        let old: Option<(String, i64)> = tx
            .query_row(
                "SELECT projection,accepted_at FROM incidents WHERE producer=? AND incident=?",
                params![producer, incident],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let accepted = now();
        let mut transition = true;
        let is_new = old.is_none();
        if let Some((data, time)) = old {
            let prior: IncidentInput = parse_stored(&data)?;
            if value.revision < prior.revision {
                return Err(Error(409, "stale_revision"));
            }
            if value.revision == prior.revision {
                if value != prior {
                    return Err(Error(409, "revision_conflict"));
                }
                return Ok((
                    200,
                    json!({"schema":SCHEMA,"producer_id":producer,"incident_id":incident,"revision":value.revision,"accepted_at":time,"idempotent":true}),
                ));
            }
            if !value.same_provenance(&prior) || value.updated_at < prior.updated_at {
                return Err(Error(409, "provenance_conflict"));
            }
            if prior.processing_state == "resolved" && value.processing_state != "resolved" {
                return Err(Error(409, "resolved_incident"));
            }
            transition = prior.processing_state != value.processing_state;
        } else {
            self.prune(tx)?;
            let count: i64 = tx.query_row("SELECT COUNT(*) FROM incidents", [], |r| r.get(0))?;
            if count >= self.config.max_incidents {
                return Err(Error(503, "capacity_exceeded"));
            }
        }
        let projection = serde_json::to_string(&value)?;
        tx.execute("INSERT INTO incidents(producer,incident,scope,state,projection,accepted_at) VALUES(?,?,?,?,?,?) ON CONFLICT(producer,incident) DO UPDATE SET state=excluded.state,projection=excluded.projection,accepted_at=excluded.accepted_at",params![producer,incident,value.receiving_scope,value.processing_state,projection,accepted])?;
        if transition {
            for route in &self.config.routes {
                if route.receiving_scope != value.receiving_scope {
                    continue;
                }
                let delivery = hash(
                    serde_json::to_string(&(
                        producer,
                        incident,
                        &value.processing_state,
                        &route.adapter_id,
                    ))?
                    .as_bytes(),
                );
                tx.execute("INSERT INTO deliveries(id,producer,incident,scope,adapter,created_at,state,projection) VALUES(?,?,?,?,?,?,'pending',?)",params![delivery,producer,incident,value.receiving_scope,route.adapter_id,accepted,projection])?;
            }
        }
        Ok((
            if is_new { 201 } else { 200 },
            json!({"schema":SCHEMA,"producer_id":producer,"incident_id":incident,"revision":value.revision,"accepted_at":accepted,"idempotent":false}),
        ))
    }
    fn prune(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute("DELETE FROM incidents WHERE state='resolved' AND accepted_at < ? AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.producer=incidents.producer AND d.incident=incidents.incident AND d.state IN ('pending','uncertain'))",[now()-self.config.retention_seconds])?;
        Ok(())
    }
    fn incident(
        &self,
        tx: &Transaction<'_>,
        actor: &Actor,
        producer: &str,
        incident: &str,
    ) -> Result<Value> {
        let (data,time,scope): (String,i64,String) = tx.query_row("SELECT projection,accepted_at,scope FROM incidents WHERE producer=? AND incident=?",params![producer,incident],|r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?.ok_or(Error(404,"not_found"))?;
        if !actor.scope(&scope) {
            return Err(Error(404, "not_found"));
        }
        let mut stmt = tx.prepare("SELECT actor,request,at FROM acknowledgments WHERE producer=? AND incident=? ORDER BY at,actor,request")?;
        let acknowledgments = stmt.query_map(params![producer,incident],|r| Ok(json!({"actor_id":r.get::<_,String>(0)?,"request_id":r.get::<_,String>(1)?,"acknowledged_at":r.get::<_,i64>(2)?})))?.collect::<std::result::Result<Vec<_>,_>>()?;
        let mut stmt =
            tx.prepare("SELECT id FROM deliveries WHERE producer=? AND incident=? ORDER BY seq")?;
        let ids = stmt
            .query_map(params![producer, incident], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let deliveries: Vec<Value> = ids
            .iter()
            .map(|id| self.delivery(tx, id))
            .collect::<Result<_>>()?;
        Ok(
            json!({"projection":parse_stored::<Value>(&data)?,"accepted_at":time,"observed_at":time,"freshness":if now()-time<=self.config.freshness_seconds {"fresh"} else {"stale"},"acknowledgments":acknowledgments,"deliveries":deliveries}),
        )
    }
    fn bounds(page: &Page) -> Result<(usize, i64)> {
        let limit = page.limit.unwrap_or(50);
        let cursor = match &page.cursor {
            None => 0,
            Some(v) => v
                .parse::<i64>()
                .ok()
                .filter(|v| *v > 0)
                .ok_or(Error(400, "invalid_request"))?,
        };
        if limit == 0 || limit > 100 {
            return Err(Error(400, "invalid_request"));
        }
        Ok((limit, cursor))
    }
    fn incidents(&self, tx: &Transaction<'_>, actor: &Actor, page: Page) -> Result<(u16, Value)> {
        require(actor.grant("read"), "forbidden")?;
        let (limit, cursor) = Self::bounds(&page)?;
        if page.adapter_id.is_some()
            || page.state.is_some()
            || page.producer_id.as_ref().is_some_and(|v| !id(v))
            || page
                .processing_state
                .as_ref()
                .is_some_and(|v| !["open", "resolved"].contains(&v.as_str()))
        {
            return Err(Error(400, "invalid_request"));
        }
        // Bound scanning by the configured admission limit; never return private rows.
        let mut stmt = tx.prepare("SELECT seq,producer,incident,scope FROM incidents WHERE seq>? AND (? IS NULL OR producer=?) AND (? IS NULL OR state=?) ORDER BY seq")?;
        let rows = stmt.query_map(
            params![
                cursor,
                page.producer_id,
                page.producer_id,
                page.processing_state,
                page.processing_state
            ],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            },
        )?;
        let mut selected = Vec::new();
        for row in rows {
            let row = row?;
            if actor.scope(&row.3) {
                selected.push(row);
                if selected.len() > limit {
                    break;
                }
            }
        }
        let next = if selected.len() > limit {
            selected.pop();
            selected.last().map(|r| r.0.to_string())
        } else {
            None
        };
        let items: Vec<_> = selected
            .iter()
            .map(|r| self.incident(tx, actor, &r.1, &r.2))
            .collect::<Result<_>>()?;
        Ok((200, json!({"items":items,"next_cursor":next})))
    }
    fn ack(
        &self,
        tx: &Transaction<'_>,
        actor: &Actor,
        producer: &str,
        incident: &str,
        request: &str,
    ) -> Result<(u16, Value)> {
        require(actor.grant("acknowledge"), "forbidden")?;
        self.incident(tx, actor, producer, incident)?;
        let prior: Option<i64> = tx.query_row("SELECT at FROM acknowledgments WHERE producer=? AND incident=? AND actor=? AND request=?",params![producer,incident,actor.actor_id,request],|r| r.get(0)).optional()?;
        let at = prior.unwrap_or_else(now);
        if prior.is_none() {
            let count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM acknowledgments WHERE producer=? AND incident=?",
                params![producer, incident],
                |r| r.get(0),
            )?;
            if count >= 100 {
                return Err(Error(503, "capacity_exceeded"));
            }
            tx.execute(
                "INSERT INTO acknowledgments VALUES(?,?,?,?,?)",
                params![producer, incident, actor.actor_id, request, at],
            )?;
        }
        Ok((
            if prior.is_some() { 200 } else { 201 },
            json!({"actor_id":actor.actor_id,"request_id":request,"acknowledged_at":at}),
        ))
    }
    fn delivery(&self, tx: &Transaction<'_>, id: &str) -> Result<Value> {
        tx.query_row("SELECT id,producer,incident,scope,adapter,created_at,state,projection,request,claimed_by,claimed_at,receipt FROM deliveries WHERE id=?",[id],|r| {
            let projection: String = r.get(7)?; let receipt: Option<String> = r.get(11)?;
            let p: Value = serde_json::from_str(&projection).map_err(|e| rusqlite::Error::FromSqlConversionFailure(7,rusqlite::types::Type::Text,Box::new(e)))?;
            let receipt: Option<Value> = receipt.map(|s| serde_json::from_str(&s)).transpose().map_err(|e| rusqlite::Error::FromSqlConversionFailure(11,rusqlite::types::Type::Text,Box::new(e)))?;
            Ok(json!({"delivery_id":r.get::<_,String>(0)?,"producer_id":r.get::<_,String>(1)?,"incident_id":r.get::<_,String>(2)?,"receiving_scope":r.get::<_,String>(3)?,"adapter_id":r.get::<_,String>(4)?,"created_at":r.get::<_,i64>(5)?,"state":r.get::<_,String>(6)?,"projection":p,"request_id":r.get::<_,Option<String>>(8)?,"claimed_by":r.get::<_,Option<String>>(9)?,"claimed_at":r.get::<_,Option<i64>>(10)?,"receipt":receipt}))
        }).optional()?.ok_or(Error(404,"not_found"))
    }
    fn authorized_delivery(&self, tx: &Transaction<'_>, actor: &Actor, id: &str) -> Result<Value> {
        require(actor.grant("deliver"), "forbidden")?;
        let d = self.delivery(tx, id)?;
        if !actor.scope(d["receiving_scope"].as_str().unwrap_or_default())
            || !actor.adapter(d["adapter_id"].as_str().unwrap_or_default())
        {
            return Err(Error(404, "not_found"));
        }
        Ok(d)
    }
    fn deliveries(&self, tx: &Transaction<'_>, actor: &Actor, page: Page) -> Result<(u16, Value)> {
        require(actor.grant("deliver"), "forbidden")?;
        let (limit, cursor) = Self::bounds(&page)?;
        if page.producer_id.is_some()
            || page.processing_state.is_some()
            || page.state.as_ref().is_some_and(|v| {
                !["pending", "uncertain", "delivered", "failed"].contains(&v.as_str())
            })
        {
            return Err(Error(400, "invalid_request"));
        }
        if page.adapter_id.as_ref().is_some_and(|v| !actor.adapter(v)) {
            return Err(Error(403, "forbidden"));
        }
        let mut stmt = tx.prepare("SELECT seq,id,scope,adapter FROM deliveries WHERE seq>? AND (? IS NULL OR adapter=?) AND (? IS NULL OR state=?) ORDER BY seq")?;
        let rows = stmt.query_map(
            params![
                cursor,
                page.adapter_id,
                page.adapter_id,
                page.state,
                page.state
            ],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            },
        )?;
        let mut selected = Vec::new();
        for row in rows {
            let row = row?;
            if actor.scope(&row.2) && actor.adapter(&row.3) {
                selected.push(row);
                if selected.len() > limit {
                    break;
                }
            }
        }
        let next = if selected.len() > limit {
            selected.pop();
            selected.last().map(|r| r.0.to_string())
        } else {
            None
        };
        let items: Vec<_> = selected
            .iter()
            .map(|r| self.delivery(tx, &r.1))
            .collect::<Result<_>>()?;
        Ok((200, json!({"items":items,"next_cursor":next})))
    }
    fn claim(
        &self,
        tx: &Transaction<'_>,
        actor: &Actor,
        id: &str,
        value: ClaimInput,
    ) -> Result<(u16, Value)> {
        if !crate::model::id(&value.request_id) {
            return Err(Error(400, "invalid_request"));
        }
        let d = self.authorized_delivery(tx, actor, id)?;
        let allowed = d["state"] == "pending";
        if allowed {
            tx.execute("UPDATE deliveries SET state='uncertain',request=?,claimed_by=?,claimed_at=? WHERE id=? AND state='pending'",params![value.request_id,actor.actor_id,now(),id])?;
        }
        Ok((
            200,
            json!({"delivery_id":id,"request_id":value.request_id,"dispatch_allowed":allowed,"state":if allowed {"uncertain"} else {d["state"].as_str().unwrap_or("uncertain")}}),
        ))
    }
    fn receipt(
        &self,
        tx: &Transaction<'_>,
        actor: &Actor,
        id: &str,
        value: ReceiptInput,
    ) -> Result<(u16, Value)> {
        if !value.valid() {
            return Err(Error(400, "invalid_request"));
        }
        let d = self.authorized_delivery(tx, actor, id)?;
        if d["request_id"] != value.request_id || d["claimed_by"] != actor.actor_id {
            return Err(Error(409, "claim_conflict"));
        }
        let new = serde_json::to_value(&value)?;
        if !d["receipt"].is_null() && d["receipt"] == new {
            return Ok((200, d));
        }
        if d["state"] != "uncertain" {
            return Err(Error(409, "receipt_conflict"));
        }
        tx.execute(
            "UPDATE deliveries SET state=?,receipt=? WHERE id=?",
            params![value.outcome, serde_json::to_string(&value)?, id],
        )?;
        Ok((200, self.delivery(tx, id)?))
    }
    fn observe(
        &self,
        tx: &Transaction<'_>,
        actor: &Actor,
        observer: &str,
        value: ObservationInput,
    ) -> Result<(u16, Value)> {
        require(
            actor.grant("observe")
                && actor.producer(observer)
                && actor.scope(&value.receiving_scope),
            "forbidden",
        )?;
        if !id(&value.receiving_scope)
            || value.source_at.is_some_and(|v| v < 0 || v > now() + 60)
            || ![
                "healthy",
                "worker_stopped",
                "worker_stalled",
                "observation_stale",
                "observation_unreadable",
            ]
            .contains(&value.source_state.as_str())
        {
            return Err(Error(400, "invalid_request"));
        }
        let prior: Option<String> = tx
            .query_row("SELECT scope FROM observers WHERE id=?", [observer], |r| {
                r.get(0)
            })
            .optional()?;
        if prior.as_ref().is_some_and(|s| s != &value.receiving_scope) {
            return Err(Error(409, "provenance_conflict"));
        }
        let count: i64 = tx.query_row("SELECT COUNT(*) FROM observers", [], |r| r.get(0))?;
        if prior.is_none() && count >= 100 {
            return Err(Error(503, "capacity_exceeded"));
        }
        tx.execute("INSERT INTO observers VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET observed_at=excluded.observed_at,source_at=excluded.source_at,source_state=excluded.source_state",params![observer,value.receiving_scope,now(),value.source_at,value.source_state])?;
        self.status(tx, actor)
    }
    fn retire_observer(
        &self,
        tx: &Transaction<'_>,
        actor: &Actor,
        observer: &str,
        value: ObserverRetirementInput,
    ) -> Result<(u16, Value)> {
        require(
            actor.grant("observe")
                && actor.grant("status")
                && actor.producer(observer)
                && actor.scope(&value.receiving_scope),
            "forbidden",
        )?;
        if !id(&value.receiving_scope) {
            return Err(Error(400, "invalid_request"));
        }
        let prior: Option<String> = tx
            .query_row("SELECT scope FROM observers WHERE id=?", [observer], |r| {
                r.get(0)
            })
            .optional()?;
        if prior
            .as_ref()
            .is_some_and(|scope| scope != &value.receiving_scope)
        {
            return Err(Error(409, "provenance_conflict"));
        }
        // Explicit retirement removes only liveness registration. Accepted
        // incidents, work, acknowledgments and delivery receipts stay intact.
        tx.execute("DELETE FROM observers WHERE id=?", [observer])?;
        self.status(tx, actor)
    }
    pub fn backup(&self, destination: &Path) -> Result<()> {
        if destination.exists() {
            return Err(Error(409, "provenance_conflict"));
        }
        let con = self
            .connection
            .lock()
            .map_err(|_| Error(500, "internal_error"))?;
        con.backup(rusqlite::MAIN_DB, destination, None)?;
        let copy = Connection::open(destination)?;
        let check: String = copy.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        if check != "ok" {
            return Err(Error(500, "internal_error"));
        }
        Ok(())
    }
}
