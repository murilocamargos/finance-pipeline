CREATE TABLE IF NOT EXISTS wallets_audit (
    id INTEGER,
    name TEXT NOT NULL,
    currency TEXT NOT NULL,
    category TEXT,
    created_at TEXT DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    updated_by INTEGER NOT NULL,

    FOREIGN KEY (updated_by) REFERENCES jobs (id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
);