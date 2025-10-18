from __future__ import annotations
import argparse
import sqlite3
from pathlib import Path

def table_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def column_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

def add_col_if_missing(conn: sqlite3.Connection, table: str, col: str, decl: str = "TEXT") -> None:
    cur = conn.cursor()
    if not table_exists(cur, table):
        print(f"[SKIP] Table '{table}' not found; skipping column '{col}'.")
        return
    if column_exists(cur, table, col):
        print(f"[OK] {table}.{col} already exists.")
        return
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    conn.commit()
    print(f"[OK] Added column {table}.{col}")

def ensure_index(conn: sqlite3.Connection, table: str, col: str, index_name: str) -> None:
    cur = conn.cursor()
    if not table_exists(cur, table) or not column_exists(cur, table, col):
        print(f"[SKIP] Index {index_name}: table/column missing.")
        return
    cur.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}({col})")
    conn.commit()
    print(f"[OK] Ensured index {index_name}")

def apply_websites_schema(conn: sqlite3.Connection, ddl_path: Path) -> None:
    sql = ddl_path.read_text(encoding="utf-8")
    conn.executescript(sql)
    print("[OK] Applied websites DDL")

def main(db: str = "data/eopt.sqlite", ddl: str = "src/eopt/db_migrations/002_websites.sql") -> None:
    dbp = Path(db)
    ddlp = Path(ddl)
    dbp.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(dbp))
    try:
        apply_websites_schema(conn, ddlp)
        for tbl in ("rows", "snapshots"):
            add_col_if_missing(conn, tbl, "website_id", "TEXT")
            ensure_index(conn, tbl, "website_id", f"idx_{tbl}_website_id")
        print("[DONE] Migration 002 complete.")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/eopt.sqlite")
    parser.add_argument("--ddl", default="src/eopt/db_migrations/002_websites.sql")
    args = parser.parse_args()
    main(args.db, args.ddl)
