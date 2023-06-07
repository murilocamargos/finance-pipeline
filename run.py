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


def clean_transactions(cur, month):
    cur.execute('drop table if exists transactions_cl')
    cur.execute(f'''
        CREATE TABLE transactions_cl AS
        WITH
        clean_trs AS (
            SELECT
                date('{month}-' || substr('00' || tr.day, -2, 2)) as day,
                tr.value,
                tr.description,
                w.id AS wallet,
                c.id AS category,
                p.id AS place,
                e.id as event
            FROM
                transactions_raw tr
                LEFT JOIN wallets w ON w.key = tr.wallet
                LEFT JOIN categories c ON c.key = tr.category
                LEFT JOIN places p ON p.key = tr.place
                LEFT JOIN events e ON e.key = tr.event
            WHERE
                w.id IS NOT NULL
                AND c.id IS NOT NULL
                AND p.id IS NOT NULL
                AND tr.value IS NOT NULL AND tr.value <> ''
                AND (e.id IS NOT NULL OR e.id IS NULL AND (tr.event IS NULL OR tr.event = ''))
        )
        SELECT * FROM clean_trs WHERE day IS NOT NULL
    ''')


def consolidate_transactions(cur, month, job_id):
    conditions = 't.day = cl.day AND t.value = cl.value AND t.description = cl.description AND t.wallet = cl.wallet AND t.category = cl.category AND t.place = cl.place AND (t.event = cl.event OR (t.event IS NULL AND cl.event IS NULL))'
    cur.execute(f'''
        INSERT INTO transactions (day, value, description, wallet, category, place, event, created_at, created_by)
        SELECT
            cl.*, STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') as created_at, ? as created_by
        FROM
            transactions_cl cl
            LEFT JOIN transactions t ON ({conditions})
        WHERE t.id IS NULL
    ''', [job_id])

    cur.execute(f'''
        DELETE FROM transactions WHERE id IN (
            SELECT
                t.id
            FROM
                transactions t
                LEFT JOIN transactions_cl cl ON ({conditions})
            WHERE cl.day IS NULL AND t.day like '{month}-%'
        )
    ''')


def get_sheets_data(month):
    wallets = pd.read_csv('data/wallets.csv', na_filter=False)
    wallets = wallets.rename(columns={'id': 'key', 'category': 'group'})

    categories = pd.read_csv('data/categories.csv', na_filter=False)
    categories = categories.rename(columns={'id': 'key', 'category': 'group'})

    places = pd.read_csv('data/places.csv', na_filter=False)
    places['group'] = places['category'].str.split(' - ').str[0]
    places['subgroup'] = places['category'].str.split(' - ').str[1]
    places = places.rename(columns={'id': 'key'})

    events = pd.read_csv('data/events.csv', na_filter=False)
    events['starts_at'] = pd.to_datetime(events['starts_at'])
    events['ends_at'] = pd.to_datetime(events['ends_at'])
    events.loc[events['starts_at'].isna(), 'starts_at'] = '2021-01-01'
    events = events.rename(columns={'id': 'key', 'category': 'group'})

    transactions = pd.read_csv('data/transactions.csv')
    transactions.loc[transactions['description'].isna(), 'description'] = 'Comida'
    transactions['value'] = (transactions['value']*100).round().astype(int)
    transactions = transactions[pd.to_datetime(transactions['day']).dt.strftime('%Y-%m') == month].copy()

    if transactions.shape[0] != 0:
        wallets = wallets[wallets['key'].isin(transactions['wallet'].unique())]
        categories = categories[categories['key'].isin(transactions['category'].unique())]
        places = places[places['key'].isin(transactions['place'].unique())]
        events = events[events['key'].isin(transactions['event'].unique())]
    
    return wallets, categories, places, events, transactions


if __name__ == '__main__':
    month = '2023-07'
    wallets, categories, places, events, transactions = get_sheets_data(month)
    print(month, transactions.shape[0])

    with SQLite('data/db.db') as cur:
        setup_database(cur)
        job_id = job_init(cur)

        try:
            wallets_sql = upsert_dim(cur, wallets, 'wallets', (('name', None), ('group', 'category'), ('currency', None)))
            categories_sql = upsert_dim(cur, categories, 'categories', (('name', None), ('group', 'category')))
            places_sql = upsert_dim(cur, places, 'places', (('name', None), ('group', 'category'), ('subgroup', 'subcategory')))
            events_sql = upsert_dim(cur, events, 'events', (('name', None), ('group', 'category'), ('starts_at', None), ('ends_at', None)))
            transactions.to_sql(f'transactions_raw', cur.connection, if_exists='replace')
        except:
            job_fail(cur, job_id)
        
        cur.execute("begin")
        try:
            cur.execute(wallets_sql, [job_id])
            cur.execute(categories_sql, [job_id])
            cur.execute(places_sql, [job_id])
            cur.execute(events_sql, [job_id])
            clean_transactions(cur, month)
            consolidate_transactions(cur, month, job_id)
            cur.execute("commit")
        except:
            cur.execute("rollback")
            job_fail(cur, job_id)

        job_complete(cur, job_id)