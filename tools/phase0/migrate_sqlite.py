#!/usr/bin/env python3
# tools/phase0/migrate_sqlite.py
import sqlite3
from pathlib import Path

DB = Path("data/eopt.sqlite")

DDL = [
    # add columns if missing
    ("websites", "domain", "ALTER TABLE websites ADD COLUMN domain TEXT"),
    ("websites", "iso2",   "ALTER TABLE websites ADD COLUMN iso2 TEXT"),
]

INDEXES = [
    ("prices", "uq_prices",
     "CREATE UNIQUE INDEX IF NOT EXISTS uq_prices "
     "ON prices(run_id, retailer_code, product_name, net_qty_value, net_qty_unit, pack_count, source_url)"),
    ("prices", "ix_prices_country_chain",
     "CREATE INDEX IF NOT EXISTS ix_prices_country_chain "
     "ON prices(country, chain, timestamp_utc)"),
]

def has_col(cur, table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

def has_index(cur, name):
    cur.execute("PRAGMA index_list('prices')")
    return any(r[1] == name for r in cur.fetchall())

def migrate():
    if not DB.exists():
        print("[INFO] DB not found; nothing to migrate.")
        return
    con = sqlite3.connect(DB)
    cur = con.cursor()
    # columns
    for table, col, sql in DDL:
        try:
            if not has_col(cur, table, col):
                cur.execute(sql)
                print(f"[OK] Added column {table}.{col}")
        except sqlite3.OperationalError as e:
            print(f"[WARN] {table}.{col}: {e}")
    # indexes
    for table, name, sql in INDEXES:
        try:
            if not has_index(cur, name):
                cur.execute(sql)
                print(f"[OK] Ensured index {name}")
        except sqlite3.OperationalError as e:
            print(f"[WARN] index {name}: {e}")
    con.commit()
    con.close()
    print("[DONE] Migration complete.")

if __name__ == "__main__":
    migrate()
