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


def upsert_dim(cur, df, name, fields):
    df.to_sql(f'{name}_staging', cur.connection, if_exists='replace')

    insert_list, select_list, diff_list, update_list = [], [], [], []
    for old_name, new_name in fields:
        if new_name is None:
            new_name = old_name
        insert_list.append(new_name)
        select_list.append(f'ws.`{old_name}` as {new_name}')
        diff_list.append(f'ws.`{old_name}` <> w.`{new_name}`')
        update_list.append(f'{new_name}=excluded.{new_name}')
    
    return f'''
        INSERT INTO {name} (key, {', '.join(insert_list)}, updated_by)
        
        SELECT
            ws.key, {', '.join(select_list)}, ? as updated_by
        FROM
            {name}_staging ws
            LEFT JOIN {name} w ON w.key = ws.key
        WHERE
            w.id IS NULL
            OR {' OR '.join(diff_list)}
            
        ON CONFLICT(key) DO UPDATE SET
            {', '.join(update_list)},
            updated_by=excluded.updated_by,
            updated_at=STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW');
    '''


def job_fail(cur, job_id):
    cur.execute("UPDATE jobs SET failed = ?, message = ?, updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') WHERE id = ?", [1, traceback.format_exc(), job_id])
    sys.exit(1)


def job_complete(cur, job_id):
    cur.execute("UPDATE jobs SET updated_at = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') WHERE id = ?", [job_id])
    sys.exit(1)


def get_sheets_data():
    wallets = pd.DataFrame([
        {'key': 'Barclays - GBP', 'name': 'Barclays', 'currency': 'GBP', 'group': 'Corrente'},
    ])

    categories = pd.DataFrame([
        {'key': 'ALI - Bebidas (Nao Alc)', 'name': 'Bebidas (Nao Alc)', 'group': 'Alimentação'},
    ])
    
    places = pd.DataFrame([
        {'key': 'ALICAF - 1657 Chocolate House', 'name': '1657 Chocolate House', 'subgkey': 'Alimentação - Cafeteria', 'old.key': '1657 Chocolate House', 'subgroup': 'Cafeteria', 'group': 'Alimentação'},
    ])

    events = pd.DataFrame([
        {'key': 'Manchester (UK) - 26/02/22', 'name': 'Manchester (UK)', 'group': 'Lazer', 'starts_at': '26/02/2022', 'ends_at': '01/03/2022'},
    ])
    events['starts_at'] = pd.to_datetime(events['starts_at'], format='%d/%m/%Y')
    events['ends_at'] = pd.to_datetime(events['ends_at'], format='%d/%m/%Y')

    return wallets, categories, places, events


if __name__ == '__main__':
    wallets, categories, places, events = get_sheets_data()
    
    with SQLite('data/db.db') as cur:
        setup_database(cur)
        job_id = job_init(cur)

        try:
            wallets_sql = upsert_dim(cur, wallets, 'wallets', (('name', None), ('group', 'category'), ('currency', None)))
            categories_sql = upsert_dim(cur, categories, 'categories', (('name', None), ('group', 'category')))
            places_sql = upsert_dim(cur, places, 'places', (('name', None), ('group', 'category'), ('subgroup', 'subcategory')))
            events_sql = upsert_dim(cur, events, 'events', (('name', None), ('group', 'category'), ('starts_at', None), ('ends_at', None)))
        except:
            job_fail(cur, job_id)
        
        cur.execute("begin")
        try:
            cur.execute(wallets_sql, [job_id])
            cur.execute(categories_sql, [job_id])
            cur.execute(places_sql, [job_id])
            cur.execute(events_sql, [job_id])
            cur.execute("commit")
        except:
            cur.execute("rollback")
            job_fail(cur, job_id)

        job_complete(cur, job_id)