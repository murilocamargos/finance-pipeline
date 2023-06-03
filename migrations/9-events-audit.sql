CREATE TABLE IF NOT EXISTS events_audit (
    id INTEGER,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT,
    created_at TEXT DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    updated_by INTEGER NOT NULL,
    
    FOREIGN KEY (updated_by) REFERENCES jobs (id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
);