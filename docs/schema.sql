-- Applied by ensureSchema on startup. Kept here so the shape is readable
-- without walking the Go source.

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS tasks_idempotency_key_idx ON tasks (idempotency_key);
CREATE INDEX IF NOT EXISTS tasks_claim_idx ON tasks (status, visible_at, created_at);

CREATE TABLE IF NOT EXISTS dead_letters (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    attempts INT NOT NULL,
    last_error TEXT,
    dead_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
