#!/usr/bin/env python3
# tools/phase0/build_normalized_exports.py
from __future__ import annotations
import hashlib, json, os, re, sqlite3, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ---------- paths ----------
RUNS_DIR = Path("logs")
EXPORTS = Path("exports")
DATA = Path("data")
MANIFESTS = EXPORTS / "_manifests"
for p in (EXPORTS, DATA, MANIFESTS):
    p.mkdir(parents=True, exist_ok=True)

# ---------- canonical columns ----------
CANON_COLS = [
    "run_id","country","chain","retailer_code","website_id","site_domain","robots_status","mode",
    "product_name","net_qty_value","net_qty_unit","pack_count",
    "price_eur","unit_price_eur_per_l","ean","sku","source_url","timestamp_utc","store_context_json"
]

# ---------- helpers ----------
def _root_domain(u: str) -> str:
    from urllib.parse import urlparse
    host = urlparse(u or "").netloc
    if not host:
        return ""
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host

def _safe_float(x) -> Optional[float]:
    try:
        return float(str(x).replace("\u00a0"," ").replace(".","").replace(",","."))  # nl/be numbers
    except Exception:
        return None

_QTY_RX = re.compile(r"(\d+(?:[.,]\d+)?)\s*([a-zA-Z]+)")
_PACK_RX = re.compile(r"(\d+)\s*[x×]")

def parse_qty(q: str) -> Tuple[Optional[float], Optional[str], Optional[int]]:
    if not q:
        return None, None, None
    pack = None
    pm = _PACK_RX.search(q)
    if pm:
        try:
            pack = int(pm.group(1))
        except Exception:
            pack = None
    m = _QTY_RX.search(q)
    if not m:
        return None, None, pack
    val = _safe_float(m.group(1))
    unit = (m.group(2) or "").lower()
    return val, unit, pack

def liters_from(val: Optional[float], unit: Optional[str]) -> Optional[float]:
    if val is None or not unit: return None
    u = unit.lower()
    if u in ("l","lt","liter","litre","liters","litres"): return val
    if u in ("ml","milliliter","millilitre","milliliters","millilitres"): return val/1000.0
    if u in ("cl",): return val/100.0
    return None

def unit_price_per_l(val: Optional[float], unit: Optional[str], price: Optional[float]) -> Optional[float]:
    L = liters_from(val, unit)
    if L and price is not None and L > 0:
        return round(price / L, 4)
    return None

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

# ---------- retailer lookup (domain/country by code OR chain) ----------
def load_retailer_lookup() -> Dict[str, Dict[str,str]]:
    by_code: Dict[str, Dict[str,str]] = {}
    by_chain: Dict[str, Dict[str,str]] = {}
    for candidate in [Path("retailers.csv"), Path("retailers/retailers.csv"), Path("retailers/registry.csv")]:
        if not candidate.exists():
            continue
        df = pd.read_csv(candidate).fillna("")
        for _, r in df.iterrows():
            code = (r.get("code") or r.get("retailer") or "").strip()
            chain = (r.get("name") or r.get("chain") or r.get("retailer") or "").strip()
            base = (r.get("base_url") or r.get("category_url") or "").strip()
            domain = _root_domain(base)
            country = (r.get("country") or "").strip().upper()
            if code:
                by_code[code.lower()] = {"domain": domain, "country": country, "name": chain}
            if chain:
                by_chain[chain.lower()] = {"domain": domain, "country": country, "code": code}
        break
    return by_code, by_chain

# ---------- read Phase-1 rows ----------
def read_phase1_rows(run_id: str) -> pd.DataFrame:
    fourcol = EXPORTS / f"oil_prices_{run_id}.csv"
    frames: List[pd.DataFrame] = []
    if fourcol.exists():
        df = pd.read_csv(fourcol)
        df.rename(columns={"retailer":"chain"}, inplace=True)  # expected: retailer, product_name, quantity, price_eur
        for col in ("retailer_code","country","mode","robots_status","source_url","ean","sku"):
            if col not in df.columns:
                df[col] = None
        df["mode"] = df["mode"].fillna("live")
        frames.append(df[["chain","product_name","quantity","price_eur","retailer_code","country","mode","robots_status","source_url","ean","sku"]])
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=["chain","product_name","quantity","price_eur","retailer_code","country","mode","robots_status","source_url","ean","sku"])

# ---------- normalization ----------
def enrich_and_normalize(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df = df.copy()
    by_code, by_chain = load_retailer_lookup()

    # backfill retailer_code from chain if missing
    def fill_retailer_code(row) -> Optional[str]:
        code = (row.get("retailer_code") or "").strip()
        if code: return code
        ch = (row.get("chain") or "").strip().lower()
        if ch in by_chain and by_chain[ch].get("code"): return by_chain[ch]["code"]
        return None
    df["retailer_code"] = df.apply(fill_retailer_code, axis=1)

    # derive site_domain (prefer URL, else code, else chain)
    def derive_site_domain(row) -> str:
        url_dom = _root_domain(row.get("source_url") or "")
        if url_dom: return url_dom
        code = (row.get("retailer_code") or "").lower()
        if code and code in by_code and by_code[code].get("domain"): return by_code[code]["domain"]
        ch = (row.get("chain") or "").lower()
        if ch and ch in by_chain and by_chain[ch].get("domain"): return by_chain[ch]["domain"]
        return ""
    df["site_domain"] = df.apply(derive_site_domain, axis=1)

    # derive country (prefer provided, else code suffix, else lookup)
    def derive_country(row) -> Optional[str]:
        cur = (row.get("country") or "").strip().upper() or None
        if cur: return cur
        code = (row.get("retailer_code") or "").lower()
        if code.endswith("_be"): return "BE"
        if code.endswith("_nl"): return "NL"
        if code and code in by_code and by_code[code].get("country"): return by_code[code]["country"]
        ch = (row.get("chain") or "").lower()
        if ch and ch in by_chain and by_chain[ch].get("country"): return by_chain[ch]["country"]
        return None
    df["country"] = df.apply(derive_country, axis=1)

    # iso2 for website_id
    def iso_from_row(row) -> str:
        if row.get("country"): return row["country"]
        code = (row.get("retailer_code") or "").lower()
        if code.endswith("_be"): return "BE"
        if code.endswith("_nl"): return "NL"
        return "XX"
    df["iso2"] = df.apply(iso_from_row, axis=1)

    # website_id
    def make_website_id(site_domain: str, iso2: str) -> Optional[str]:
        if site_domain:
            return f"{iso2}:{site_domain}"
        return None
    df["website_id"] = [make_website_id(sd, iso) for sd, iso in zip(df["site_domain"], df["iso2"])]

    # --- normalize website_id: blank → None ---
    df["website_id"] = df["website_id"].apply(
        lambda x: (x.strip() if isinstance(x, str) and x.strip() else None)
    )

    # quantities
    vals, units, packs = [], [], []
    for q in df.get("quantity","").astype(str).tolist():
        v,u,p = parse_qty(q)
        vals.append(v); units.append(u); packs.append(p)
    df["net_qty_value"] = vals
    df["net_qty_unit"]  = [u if u else None for u in units]
    # pack_count as nullable Int64 for stable merges
    df["pack_count"]    = pd.Series(packs, dtype="Int64")

    # price + unit price
    df["price_eur"] = df["price_eur"].apply(_safe_float)
    df["unit_price_eur_per_l"] = [
        unit_price_per_l(v, u, p) for v,u,p in zip(df["net_qty_value"], df["net_qty_unit"], df["price_eur"])
    ]

    # ensure columns
    df["run_id"] = run_id
    if "ean" not in df.columns: df["ean"] = None
    if "sku" not in df.columns: df["sku"] = None
    df["timestamp_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # minimal store_context_json
    def make_ctx(row: pd.Series) -> str:
        ctx = {
            "website_id": row.get("website_id"),
            "robots_status": row.get("robots_status"),
            "mode": row.get("mode"),
            "source_domain": row.get("site_domain"),
        }
        return json.dumps(ctx, ensure_ascii=False)
    df["store_context_json"] = df.apply(make_ctx, axis=1)

    # keep canonical order
    for c in CANON_COLS:
        if c not in df.columns:
            df[c] = None
    return df[CANON_COLS]

# ---------- Excel helpers ----------
def pivot_country_chain(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=["country","chain","rows","avg_price_eur","id_rate"])
    tmp = df.assign(has_id=df["ean"].notna() | df["sku"].notna())
    gp = tmp.groupby(["country","chain"], dropna=False)
    return gp.agg(rows=("product_name","size"), avg_price_eur=("price_eur","mean"), id_rate=("has_id","mean")).reset_index()

def coverage(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=["country","chain","rows"])
    return df.groupby(["country","chain"], dropna=False).size().reset_index(name="rows")

def _coerce_change_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ("net_qty_value","price_eur","unit_price_eur_per_l"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in ("retailer_code","product_name","net_qty_unit"):
        if c in out.columns:
            out[c] = out[c].astype("string")
    if "pack_count" in out.columns:
        out["pack_count"] = out["pack_count"].astype("Int64")
    return out

def changes(prev_all: Optional[pd.DataFrame], cur_all: pd.DataFrame) -> pd.DataFrame:
    cols = ["retailer_code","product_name","net_qty_value","net_qty_unit","pack_count","price_eur"]
    if prev_all is None or prev_all.empty or cur_all.empty:
        return pd.DataFrame(columns=["retailer_code","product_name","net_qty_value","net_qty_unit","pack_count","prev_price","cur_price","delta"])
    A = _coerce_change_keys(prev_all[cols].copy()).rename(columns={"price_eur":"prev_price"})
    B = _coerce_change_keys(cur_all[cols].copy()).rename(columns={"price_eur":"cur_price"})
    keys = ["retailer_code","product_name","net_qty_value","net_qty_unit","pack_count"]
    merged = pd.merge(B, A, on=keys, how="left")
    merged["delta"] = merged["cur_price"] - merged["prev_price"]
    return merged.sort_values(by="delta", ascending=False)

def qa_sheet(df: pd.DataFrame) -> pd.DataFrame:
    issues: List[Dict[str,Any]] = []
    for i, r in df.iterrows():
        if not r.get("website_id"):
            issues.append({"row": i, "issue": "website_id null", "source_url": r["source_url"], "retailer_code": r["retailer_code"]})
        if r["unit_price_eur_per_l"] is not None and not (1 <= r["unit_price_eur_per_l"] <= 200):
            issues.append({"row": i, "issue": "unit price out of bounds", "value": r["unit_price_eur_per_l"], "name": r["product_name"]})
        if pd.isna(r["product_name"]) or str(r["product_name"]).strip()=="":
            issues.append({"row": i, "issue": "missing product_name"})
    return pd.DataFrame(issues)

def suspect_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=df.columns)
    return df[(df["unit_price_eur_per_l"].notna()) & ((df["unit_price_eur_per_l"] < 1) | (df["unit_price_eur_per_l"] > 200))].copy()

def write_weekly_master(df: pd.DataFrame, run_id: str) -> Tuple[Path, Path]:
    weekly = EXPORTS / f"oils-prices_{run_id}.xlsx"
    master = EXPORTS / "oils-prices_MASTER.xlsx"

    cov = coverage(df)
    piv = pivot_country_chain(df)
    sus = suspect_sheet(df)
    qa  = qa_sheet(df)

    with pd.ExcelWriter(weekly, engine="openpyxl") as xw:
        df.to_excel(xw, sheet_name="All_Data", index=False)
        cov.to_excel(xw, sheet_name="Coverage", index=False)
        piv.to_excel(xw, sheet_name="Pivots_Country_Chain", index=False)
        sus.to_excel(xw, sheet_name="Suspect", index=False)
        qa.to_excel(xw, sheet_name="QA", index=False)
        pd.DataFrame(columns=["retailer_code","product_name","net_qty_value","net_qty_unit","pack_count","prev_price","cur_price","delta"]).to_excel(
            xw, sheet_name="Changes", index=False
        )

    if master.exists():
        md = pd.read_excel(master, sheet_name=None)
        all_old = md.get("All_Data", pd.DataFrame(columns=CANON_COLS))
        all_new = pd.concat([all_old, df], ignore_index=True)
        try:
            chg = changes(all_old, df)
        except Exception:
            chg = pd.DataFrame(columns=["retailer_code","product_name","net_qty_value","net_qty_unit","pack_count","prev_price","cur_price","delta"])
        with pd.ExcelWriter(master, engine="openpyxl") as xw:
            all_new.to_excel(xw, sheet_name="All_Data", index=False)
            coverage(all_new).to_excel(xw, sheet_name="Coverage", index=False)
            pivot_country_chain(all_new).to_excel(xw, sheet_name="Pivots_Country_Chain", index=False)
            suspect_sheet(all_new).to_excel(xw, sheet_name="Suspect", index=False)
            qa_sheet(all_new).to_excel(xw, sheet_name="QA", index=False)
            chg.to_excel(xw, sheet_name="Changes", index=False)
    else:
        with pd.ExcelWriter(master, engine="openpyxl") as xw:
            df.to_excel(xw, sheet_name="All_Data", index=False)
            cov.to_excel(xw, sheet_name="Coverage", index=False)
            piv.to_excel(xw, sheet_name="Pivots_Country_Chain", index=False)
            sus.to_excel(xw, sheet_name="Suspect", index=False)
            qa.to_excel(xw, sheet_name="QA", index=False)
            pd.DataFrame(columns=["retailer_code","product_name","net_qty_value","net_qty_unit","pack_count","prev_price","cur_price","delta"]).to_excel(
                xw, sheet_name="Changes", index=False
            )
    return weekly, master

# ---------- SQLite with FKs ----------
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS websites (
    website_id TEXT PRIMARY KEY,
    domain TEXT,
    iso2 TEXT
);
CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    country TEXT,
    chain TEXT,
    retailer_code TEXT,
    website_id TEXT NOT NULL,
    site_domain TEXT,
    robots_status TEXT,
    mode TEXT,
    product_name TEXT,
    net_qty_value REAL,
    net_qty_unit TEXT,
    pack_count INTEGER,
    price_eur REAL,
    unit_price_eur_per_l REAL,
    ean TEXT,
    sku TEXT,
    source_url TEXT,
    timestamp_utc TEXT,
    store_context_json TEXT,
    FOREIGN KEY (website_id) REFERENCES websites(website_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_prices ON prices(run_id, retailer_code, product_name, net_qty_value, net_qty_unit, pack_count, source_url);
CREATE INDEX IF NOT EXISTS ix_prices_country_chain ON prices(country, chain, timestamp_utc);
"""

def upsert_sqlite(df: pd.DataFrame):
    db = DATA / "eopt.sqlite"
    con = sqlite3.connect(db)
    cur = con.cursor()

    # Always enable FK checks for THIS connection
    cur.execute("PRAGMA foreign_keys = ON;")

    # Create/upgrade schema
    for stmt in SCHEMA_SQL.strip().split(";"):
        s = stmt.strip()
        if s:
            cur.execute(s)
    con.commit()

    # Normalize website_id consistently (strip spaces, uppercase ISO)
    def _norm_webid(x: Optional[str]) -> Optional[str]:
        if not isinstance(x, str):
            return None
        x = x.strip()
        if not x:
            return None
        if ":" in x:
            iso, dom = x.split(":", 1)
            return f"{(iso or '').upper()}:{(dom or '').strip().lower()}"
        return x

    df = df.copy()
    df["website_id"] = df["website_id"].apply(_norm_webid)
    df["site_domain"] = df["site_domain"].astype(str).str.strip().str.lower()

    # 1) Upsert websites
    web_rows = (
        df[["website_id","site_domain"]]
        .dropna(subset=["website_id"])
        .drop_duplicates()
        .rename(columns={"site_domain":"domain"})
        .assign(iso2=lambda x: x["website_id"].str.split(":").str[0])
    )

    for _, row in web_rows.iterrows():
        cur.execute(
            "INSERT OR IGNORE INTO websites(website_id, domain, iso2) VALUES (?,?,?)",
            (row["website_id"], row["domain"], row["iso2"]),
        )
    con.commit()

    # 2) Validate FKs BEFORE inserting prices
    cur.execute("SELECT website_id FROM websites")
    valid_webids = {r[0] for r in cur.fetchall()}

    prices_df = df.dropna(subset=["website_id"]).copy()
    mask_valid = prices_df["website_id"].isin(valid_webids)
    bad = prices_df.loc[~mask_valid, ["website_id","site_domain","source_url"]].drop_duplicates()

    if not bad.empty:
        # Write a small debug file to help diagnose
        debug_path = EXPORTS / "_debug_missing_webids.csv"
        bad.to_csv(debug_path, index=False, encoding="utf-8")
        print(f"[WARN] {len(bad)} row(s) reference website_id not present in websites. "
              f"Details → {debug_path}")

    # Keep only FK-valid rows
    prices_df = prices_df.loc[mask_valid].copy()

    # 3) Insert prices safely
    if not prices_df.empty:
        prices_df.to_sql("prices", con, if_exists="append", index=False)
    else:
        print("[WARN] No FK-valid price rows to insert.")

    con.close()

# ---------- manifest ----------
def write_manifest(run_id: str, weekly: Path, master: Path, rows: int, inputs: Dict[str, Any]) -> Path:
    m = {
        "run_id": run_id,
        "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "inputs": inputs,
        "artifacts": {
            "weekly_xlsx": {"path": str(weekly), "sha256": sha256_file(weekly) if weekly.exists() else None},
            "master_xlsx": {"path": str(master), "sha256": sha256_file(master) if master.exists() else None},
        },
        "row_count": rows,
        "git_commit": os.getenv("GIT_COMMIT", None),
        "settings": {"phase": "real","bounds_eur_per_l": [1,200]},
    }
    out = MANIFESTS / f"run_{run_id}.json"
    out.write_text(json.dumps(m, indent=2), encoding="utf-8")
    return out

# ---------- main ----------
def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/phase0/build_normalized_exports.py <RUN_ID>")
        sys.exit(2)
    run_id = sys.argv[1]

    df = read_phase1_rows(run_id)
    if df.empty:
        print(f"[WARN] No Phase-1 rows found for run {run_id}.")
        sys.exit(0)

    df_norm = enrich_and_normalize(df, run_id)
    weekly, master = write_weekly_master(df_norm, run_id)
    upsert_sqlite(df_norm)

    inputs = {
        "fourcol_csv": str((EXPORTS / f"oil_prices_{run_id}.csv").resolve()),
        "retailers_csv": str(Path("retailers.csv").resolve()) if Path("retailers.csv").exists() else None,
    }
    manifest = write_manifest(run_id, weekly, master, len(df_norm), inputs)

    print(f"[OK] Weekly  → {weekly}")
    print(f"[OK] MASTER  → {master}")
    print(f"[OK] SQLite  → {DATA/'eopt.sqlite'}")
    print(f"[OK] Manifest→ {manifest}")
    print(f"[OK] Rows    → {len(df_norm)}")

if __name__ == "__main__":
    main()
