import csv
import json
import os
import sqlite3
import threading
from contextlib import contextmanager

DB_LOCK = threading.RLock()

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    max_concurrent_calls INTEGER NOT NULL,
    webhook_secret TEXT
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    allowed_days TEXT NOT NULL,
    webhook_secret TEXT
);

CREATE TABLE IF NOT EXISTS dnd_registry (
    phone TEXT PRIMARY KEY,
    recorded_at_sim TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leads (
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    -- States: PENDING, RETRY_WAIT, CALLBACK_WAIT, DIALING_INTENT, ACTIVE, COMPLETED, EXHAUSTED, DND_BLOCKED
    state TEXT NOT NULL DEFAULT 'PENDING',
    prior_wait_state TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_eligible_sim REAL NOT NULL DEFAULT 0.0,
    last_status TEXT DEFAULT '',
    last_disposition TEXT DEFAULT '',
    last_ended_at_sim REAL DEFAULT 0.0,
    active_request_id TEXT DEFAULT NULL,
    PRIMARY KEY (campaign_id, lead_id)
);

CREATE TABLE IF NOT EXISTS call_attempts (
    request_id TEXT PRIMARY KEY,
    call_sid TEXT DEFAULT NULL,
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    -- States: INTENT, ACTIVE, ENDED, ABORTED
    state TEXT NOT NULL DEFAULT 'INTENT',
    counted_attempt INTEGER NOT NULL DEFAULT 0,
    in_flight_pid INTEGER DEFAULT NULL,
    created_real_ts REAL NOT NULL,
    status TEXT DEFAULT NULL,
    disposition TEXT DEFAULT NULL,
    ended_at_sim TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_leads_state_elig ON leads(state, next_eligible_sim);
CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
CREATE INDEX IF NOT EXISTS idx_ca_state ON call_attempts(state);
CREATE INDEX IF NOT EXISTS idx_ca_phone_state ON call_attempts(phone, state);
CREATE INDEX IF NOT EXISTS idx_ca_tenant_state ON call_attempts(tenant_id, state);
"""


class Database:
    """Thread-safe SQLite WAL database with BEGIN IMMEDIATE transaction support."""

    def __init__(self, db_path: str = "dispatcher.db"):
        self.db_path = db_path
        self._conn = sqlite3.connect(
            self.db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with DB_LOCK:
            self._conn.executescript(SCHEMA_SQL)

    @contextmanager
    def transaction(self):
        with DB_LOCK:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def close(self):
        with DB_LOCK:
            self._conn.close()


def load_fixtures(db: Database, fixtures_path: str) -> dict:
    """
    Idempotently loads tenants, campaigns, and leads from campaigns.json and its
    referenced CSV files. Existing lead rows are preserved via INSERT OR IGNORE
    so crash restarts never overwrite in-progress or completed lead state.
    Returns the parsed campaigns.json configuration dictionary.
    """
    abs_path = os.path.abspath(fixtures_path)
    base_dir = os.path.dirname(abs_path)
    with open(abs_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    webhook_secret = cfg.get("webhook_secret", "")

    with db.transaction() as conn:
        for t in cfg.get("tenants", []):
            conn.execute(
                """
                INSERT INTO tenants (tenant_id, max_concurrent_calls, webhook_secret)
                VALUES (?, ?, ?)
                ON CONFLICT(tenant_id) DO UPDATE SET
                    max_concurrent_calls = excluded.max_concurrent_calls,
                    webhook_secret = excluded.webhook_secret
                """,
                (t["tenant_id"], int(t["max_concurrent_calls"]), webhook_secret),
            )

        for c in cfg.get("campaigns", []):
            win = c["window"]
            conn.execute(
                """
                INSERT INTO campaigns (
                    campaign_id, tenant_id, max_attempts,
                    window_start, window_end, allowed_days, webhook_secret
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(campaign_id) DO UPDATE SET
                    tenant_id = excluded.tenant_id,
                    max_attempts = excluded.max_attempts,
                    window_start = excluded.window_start,
                    window_end = excluded.window_end,
                    allowed_days = excluded.allowed_days,
                    webhook_secret = excluded.webhook_secret
                """,
                (
                    c["campaign_id"],
                    c["tenant_id"],
                    int(c.get("max_attempts", 3)),
                    win["start"],
                    win["end"],
                    json.dumps(win["days"]),
                    webhook_secret,
                ),
            )

            leads_csv_path = os.path.join(base_dir, c["leads_file"])
            with open(leads_csv_path, "r", encoding="utf-8", newline="") as lf:
                reader = csv.DictReader(lf)
                rows = [
                    (c["campaign_id"], row["lead_id"], c["tenant_id"], row["phone"])
                    for row in reader
                ]
            conn.executemany(
                """
                INSERT OR IGNORE INTO leads (campaign_id, lead_id, tenant_id, phone)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )

    return cfg

