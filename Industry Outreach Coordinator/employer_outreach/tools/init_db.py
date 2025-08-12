# tools/init_db.py
from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "db" / "outreach.sqlite"
DB.parent.mkdir(parents=True, exist_ok=True)

DDL = """
CREATE TABLE IF NOT EXISTS companies (
  company_id TEXT PRIMARY KEY,
  legal_name TEXT, known_as TEXT, domain TEXT, naics TEXT,
  province TEXT, city TEXT, general_email TEXT,
  source TEXT, source_url TEXT, program TEXT,
  fetched_at TEXT, schema_version INTEGER
);
CREATE TABLE IF NOT EXISTS contacts (
  contact_id TEXT PRIMARY KEY,
  company_id TEXT, full_name TEXT, title TEXT, email TEXT, email_type TEXT,
  source_url TEXT, snapshot_path TEXT, casl_basis TEXT,
  fetched_at TEXT, schema_version INTEGER
);
"""

con = sqlite3.connect(str(DB))
con.executescript(DDL)
con.commit()
con.close()
print(f"DB ready at {DB}")
