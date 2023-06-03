CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT,
    created_at TEXT DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    updated_at TEXT DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    updated_by INTEGER NOT NULL,
    
    UNIQUE(name, starts_at)
    FOREIGN KEY (updated_by) REFERENCES jobs (id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
);