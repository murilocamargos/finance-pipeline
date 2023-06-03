CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    uuid TEXT NOT NULL,
    created_at TEXT DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    updated_at TEXT DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    last_step INTEGER DEFAULT 0
);