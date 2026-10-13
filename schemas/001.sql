CREATE TABLE incidents (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    producer TEXT NOT NULL, incident TEXT NOT NULL, scope TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('open','resolved')),
    projection TEXT NOT NULL, accepted_at INTEGER NOT NULL,
    UNIQUE(producer,incident)
);
CREATE INDEX incidents_scope_seq ON incidents(scope,seq);
CREATE TABLE acknowledgments (
    producer TEXT NOT NULL, incident TEXT NOT NULL, actor TEXT NOT NULL,
    request TEXT NOT NULL, at INTEGER NOT NULL,
    PRIMARY KEY(producer,incident,actor,request),
    FOREIGN KEY(producer,incident) REFERENCES incidents(producer,incident) ON DELETE CASCADE
);
CREATE TABLE deliveries (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
    producer TEXT NOT NULL, incident TEXT NOT NULL, scope TEXT NOT NULL,
    adapter TEXT NOT NULL, created_at INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','uncertain','delivered','failed')),
    projection TEXT NOT NULL, request TEXT, claimed_by TEXT, claimed_at INTEGER, receipt TEXT,
    FOREIGN KEY(producer,incident) REFERENCES incidents(producer,incident) ON DELETE CASCADE
);
CREATE INDEX deliveries_scope_adapter_seq ON deliveries(scope,adapter,seq);
CREATE TABLE observers (
    id TEXT PRIMARY KEY, scope TEXT NOT NULL, observed_at INTEGER NOT NULL,
    source_at INTEGER, source_state TEXT NOT NULL
);
PRAGMA user_version=1;
