-- Shared schema. Changes here affect everyone: announce them in the group chat.
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS sensor_events (
  event_id    TEXT PRIMARY KEY,
  node_id     TEXT NOT NULL,
  sensor      TEXT NOT NULL,
  value       REAL NOT NULL,
  unit        TEXT,
  observed_at REAL NOT NULL,      -- event time (wall clock, set by the sensor)
  received_at REAL                -- receipt time (set by the Processor)
);
CREATE INDEX IF NOT EXISTS idx_events_observed ON sensor_events(observed_at);

CREATE TABLE IF NOT EXISTS edge_decisions (
  decision_id   TEXT PRIMARY KEY,
  event_id      TEXT NOT NULL REFERENCES sensor_events(event_id),
  rule          TEXT,
  actuator      TEXT,
  command_value REAL,
  issued_mono   REAL,             -- time.monotonic(); only meaningful since last boot
  expires_mono  REAL,             -- issued_mono + ttl
  status        TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | DISPATCHED | EXPIRED
  created_at    REAL
);

CREATE TABLE IF NOT EXISTS outbox (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id    TEXT NOT NULL UNIQUE,
  payload     TEXT NOT NULL,
  enqueued_at REAL NOT NULL,
  sent_at     REAL                -- NULL = still waiting for the WAN
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(sent_at);

CREATE TABLE IF NOT EXISTS process_health (
  name            TEXT PRIMARY KEY,
  pid             INTEGER,
  state           TEXT,
  rss_kb          INTEGER,
  cpu_pct         REAL,
  ctx_voluntary   INTEGER,
  ctx_involuntary INTEGER,
  restarts        INTEGER,
  updated_at      REAL
);

-- Mock upstream receiver. PRIMARY KEY on event_id gives idempotent delivery.
CREATE TABLE IF NOT EXISTS upstream_sink (
  event_id    TEXT PRIMARY KEY,
  payload     TEXT NOT NULL,
  received_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS kv (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- ---- Durable admission and command intents (final-term crash-window fix) ----
-- SensorGen admits every reading here BEFORE it enters the in-memory queue, so an event taken
-- off the queue by a Processor that is then SIGKILLed is not lost: the restarted Processor
-- replays every row whose processed_at is still NULL.
CREATE TABLE IF NOT EXISTS ingress (
  event_id     TEXT PRIMARY KEY,
  payload      TEXT NOT NULL,
  admitted_at  REAL NOT NULL,
  processed_at REAL               -- set in the SAME transaction that commits the event
);
CREATE INDEX IF NOT EXISTS idx_ingress_pending ON ingress(processed_at);

-- Simulated actuator state (the "effect projection") and one receipt per applied command.
-- Both are written in the same transaction that marks the decision DISPATCHED, so after a
-- crash an applied command always has its receipt and is never applied twice.
CREATE TABLE IF NOT EXISTS actuator_state (
  actuator    TEXT PRIMARY KEY,
  value       REAL,
  last_seq    INTEGER NOT NULL,   -- only a command with a higher seq may change the state
  decision_id TEXT,
  boot_epoch  TEXT,
  applied_at  REAL
);

CREATE TABLE IF NOT EXISTS actuator_receipts (
  decision_id TEXT PRIMARY KEY,
  actuator    TEXT NOT NULL,
  value       REAL,
  seq         INTEGER NOT NULL,
  applied_at  REAL NOT NULL
);
