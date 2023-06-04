CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY,
    day TEXT NOT NULL,
    value INTEGER NOT NULL,
    description TEXT NOT NULL,
    wallet INTEGER NOT NULL,
    category INTEGER NOT NULL,
    place INTEGER NOT NULL,
    event INTEGER,
    created_at TEXT DEFAULT (STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    created_by INTEGER NOT NULL,
    
    FOREIGN KEY (created_by) REFERENCES jobs (id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
    
    FOREIGN KEY (wallet) REFERENCES wallets (id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
    
    FOREIGN KEY (category) REFERENCES categories (id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
    
    FOREIGN KEY (place) REFERENCES places (id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
    
    FOREIGN KEY (event) REFERENCES events (id)
        ON UPDATE SET NULL
        ON DELETE SET NULL
);