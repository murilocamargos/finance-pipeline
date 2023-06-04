import sqlite3
from pathlib import Path
import pandas as pd
import traceback
import sys


class SQLite():
    def __init__(self, file='sqlite.db'):
        self.file = file
    
    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        self.conn.row_factory = sqlite3.Row
        return self.conn.cursor()
    
    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


def setup_database(cur):
    for sql in Path('./migrations').glob('*.sql'):
        cur.execute(sql.read_text())


def job_init(cur):
    cur.execute('INSERT INTO jobs DEFAULT VALUES')
    return cur.lastrowid


def upsert_wallets(cur, df):
    df.to_sql('wallets_staging', cur.connection, if_exists='replace')
    
    return '''
        INSERT INTO wallets (key, name, currency, category, updated_by)
        
        SELECT
            ws.key, ws.name, ws.currency, ws.`group` as category, ? as updated_by
        FROM
            wallets_staging ws
            LEFT JOIN wallets w ON w.key = ws.key
        WHERE
            w.id IS NULL
            OR ws.`group` <> w.category
            OR ws.name <> w.name
            OR ws.currency <> w.currency
            
        ON CONFLICT(key) DO UPDATE SET
            category=excluded.category,
            name=excluded.name,
            currency=excluded.currency,
            updated_by=excluded.updated_by,
            updated_at=STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW');
    '''

def upsert_categories(cur, df):
    df.to_sql('categories_staging', cur.connection, if_exists='replace')
    
    return '''
        INSERT INTO categoriess (key, name, category, updated_by)
        
        SELECT
            ws.key, ws.name, ws.`group` as category, ? as updated_by
        FROM
            categories_staging ws
            LEFT JOIN categories w ON w.key = ws.key
        WHERE
            w.id IS NULL
            OR ws.`group` <> w.category
            OR ws.name <> w.name
            
        ON CONFLICT(key) DO UPDATE SET
            category=excluded.category,
            name=excluded.name,
            updated_by=excluded.updated_by,
            updated_at=STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW');
    '''


def job_fail(cur, job_id):
    cur.execute("UPDATE jobs SET failed = ?, message = ?, updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') WHERE id = ?", [1, traceback.format_exc(), job_id])
    sys.exit(1)

def job_complete(cur, job_id):
    cur.execute("UPDATE jobs SET updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') WHERE id = ?", [job_id])
    sys.exit(1)

if __name__ == '__main__':
    wallets = pd.DataFrame([
        {'key': 'Barclays - GBP', 'name': 'Barclays', 'currency': 'GBP', 'group': 'Corrente'},
    ])

    categories = pd.DataFrame([
        {'key': 'ALI - Bebidas (Nao Alc)', 'name': 'Bebidas (Nao Alc)', 'group': 'Alimentação'},
    ])
    
    with SQLite('data/db.db') as cur:
        setup_database(cur)
        job_id = job_init(cur)

        try:
            wallets_sql = upsert_wallets(cur, wallets)
            categories_sql = upsert_categories(cur, categories)
        except:
            job_fail(cur, job_id)
        
        cur.execute("begin")
        try:
            cur.execute(wallets_sql, [job_id])
            cur.execute(categories_sql, [job_id])
            cur.execute("commit")
        except:
            cur.execute("rollback")
            job_fail(cur, job_id)

        job_complete(cur, job_id)