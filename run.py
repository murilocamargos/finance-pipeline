import sqlite3
from pathlib import Path


def setup_database(cur):
    for sql in Path('./migrations').glob('*.sql'):
        cur.execute(sql.read_text())


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


if __name__ == '__main__':
    with SQLite('data/db.db') as cur:
        setup_database(cur)