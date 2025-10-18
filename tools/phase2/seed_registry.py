from __future__ import annotations
import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml

from eopt.ids import make_website_id, _root_domain

# --------------------------
# Phase gates for manifests
# --------------------------
GATES = {
    "min_retailers_active": 2,
    "max_zero_result_retailers": 0,
    "ean_or_sku_presence_rate": 0.60,
    "unit_price_sanity": {"eur_per_l": [1, 200]},
    "wow_drop_threshold_pct": 70,
}

# --------------------------------------------
# Canonical class mapping (tokens + overrides)
# --------------------------------------------
ALLOWED_CLASSES = {"grocery", "beauty", "pharmacy", "marketplace"}

DEFAULT_CLASS_TOKENS = {
    "grocery": [
        "grocery", "grocer", "supermarket", "supermarkt", "supermarch",
        "alimentation", "food", "hypermarket", "hyper", "market",
        "superstore", "drive", "boodschappen", "courses"
    ],
    "pharmacy": [
        "pharmacy", "pharmacie", "apotheek", "parapharmacie",
        "parapharmacy", "farmacia", "pharma", "parapharmacie"
    ],
    "beauty": [
        "beauty", "cosmetic", "cosmetica", "parfum", "perfume", "parfumerie",
        "drugstore", "drogerie", "drogist", "drogisterij", "h&b",
        "health & beauty", "health and beauty", "hnb", "higiene", "hygiene"
    ],
    "marketplace": [
        "marketplace", "platform"
    ],
}
# Optional direct exact/alias mapping: e.g., {"drugstore": "beauty"}
DEFAULT_DIRECT_MAP: Dict[str, str] = {}

def _normalize_text(s: str) -> str:
    s = (s or "").lower()
    # normalize common punctuation/spaces so tokens match
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9\s\-\.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_class_map(path: Optional[Path]) -> Dict[str, List[str] | Dict[str, str]]:
    """
    Load optional retailers/class_map.yaml with structure:
      grocery: [tokens...]
      pharmacy: [tokens...]
      beauty: [tokens...]
      marketplace: [tokens...]
      map: { "drugstore": "beauty", "parapharmacie": "pharmacy", ... }
    """
    tokens = {k: list(v) for k, v in DEFAULT_CLASS_TOKENS.items()}
    direct = dict(DEFAULT_DIRECT_MAP)

    if path and path.exists():
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for cls in ("grocery", "pharmacy", "beauty", "marketplace"):
            if isinstance(data.get(cls), list):
                tokens[cls].extend([_normalize_text(t) for t in data[cls]])
        if isinstance(data.get("map"), dict):
            for k, v in data["map"].items():
                v_norm = _normalize_text(str(v))
                if v_norm in ALLOWED_CLASSES:
                    direct[_normalize_text(str(k))] = v_norm

    # de-duplicate
    for k in tokens:
        seen = []
        for t in tokens[k]:
            t = _normalize_text(t)
            if t and t not in seen:
                seen.append(t)
        tokens[k] = seen

    # sanity: direct map points only to allowed classes
    direct = { _normalize_text(k): v for k, v in direct.items() if v in ALLOWED_CLASSES }
    return {"tokens": tokens, "map": direct}

def canonical_class(value: str, default_hint: str, class_map: Dict) -> str:
    s = _normalize_text(value)
    # 1) exact/alias map first
    direct: Dict[str, str] = class_map.get("map", {})
    if s in direct:
        return direct[s]
    # 2) token membership
    tokens: Dict[str, List[str]] = class_map.get("tokens", {})
    def has_any(tok_list: List[str]) -> bool:
        return any(t for t in tok_list if t and t in s)
    if has_any(tokens.get("grocery", [])): return "grocery"
    if has_any(tokens.get("pharmacy", [])): return "pharmacy"
    if has_any(tokens.get("beauty", [])):   return "beauty"
    if has_any(tokens.get("marketplace", [])): return "marketplace"
    # 3) fallback to the sheet group default (grocery for grocers file, pharmacy for beauty/pharmacy file)
    return default_hint if default_hint in ALLOWED_CLASSES else "grocery"

# --------------------------------------------------
# Flexible column name variants (lowercased, stripped)
# NOTE: prefer ISO2 over full Country names.
# --------------------------------------------------
COL_VARIANTS = {
    "country": ["iso2", "country", "iso_2", "cc", "land", "nation"],
    "chain":   ["chain", "platform", "retailer", "name", "brand", "company", "store", "shop", "shop_name"],
    "site_domain": [
        "site_domain", "domain", "root_domain", "website", "homepage",
        "host", "base_url", "url", "site", "shop_url",
    ],
    "retailer_class": ["retailer_class", "class", "category", "vertical", "type"],
    "priority": ["priority", "tier", "must_cover", "coverage_priority"],
}

def to_root_domain(url_or_host: str) -> str:
    from urllib.parse import urlparse
    s = (str(url_or_host) or "").strip()
    if not s:
        return ""
    host = urlparse(s).netloc if "://" in s else s
    return _root_domain(host)

def _normalize_columns(
    df: pd.DataFrame,
    override_country: Optional[str] = None,
    override_chain: Optional[str] = None,
    override_domain: Optional[str] = None,
) -> Dict[str, str]:
    """Return mapping canonical_name -> actual_column in df (case/space-insensitive)."""
    lower_map = {c.lower().strip(): c for c in df.columns}
    out: Dict[str, str] = {}

    def resolve(want: str, override: Optional[str]) -> Optional[str]:
        if override:
            key = override.lower().strip()
            if key not in lower_map:
                cols = ", ".join(df.columns)
                raise KeyError(
                    f"Override '{override}' for '{want}' not found in sheet. "
                    f"Available columns: {cols}"
                )
            return lower_map[key]
        for v in COL_VARIANTS[want]:
            if v in lower_map:
                return lower_map[v]
        return None

    out["country"] = resolve("country", override_country)
    out["chain"] = resolve("chain", override_chain)
    out["site_domain"] = resolve("site_domain", override_domain)

    hard_missing = [k for k in ("country", "chain", "site_domain") if not out.get(k)]
    if hard_missing:
        cols = ", ".join(df.columns)
        msg = ["Seed sheet is missing required columns:"]
        for k in hard_missing:
            msg.append(f" - {k}: tried {COL_VARIANTS[k]}")
        msg.append(f"Available columns: {cols}")
        raise KeyError("\n".join(msg))

    # optional fields
    rc = resolve("retailer_class", None)
    pr = resolve("priority", None)
    if rc: out["retailer_class"] = rc
    if pr: out["priority"] = pr
    return out

def load_sheet(fp: Path, sheet_name: str | None) -> pd.DataFrame:
    xl = pd.ExcelFile(fp)
    if sheet_name and sheet_name in xl.sheet_names:
        return xl.parse(sheet_name)
    return xl.parse(xl.sheet_names[0])

def load_seeds(
    grocers_xlsx: Path,
    beauty_xlsx: Path,
    grocers_sheet: str | None,
    beauty_sheet: str | None,
    country_col: Optional[str],
    chain_col: Optional[str],
    domain_col: Optional[str],
    class_map: Dict,
) -> pd.DataFrame:
    frames = []
    for fp, default_class, sname in [
        (grocers_xlsx, "grocery",  grocers_sheet),
        (beauty_xlsx,  "pharmacy", beauty_sheet),
    ]:
        df_raw = load_sheet(fp, sname)
        colmap = _normalize_columns(
            df_raw,
            override_country=country_col,
            override_chain=chain_col,
            override_domain=domain_col,
        )
        out = pd.DataFrame({
            "country": df_raw[colmap["country"]].astype(str).str.upper().str.strip(),
            "chain":   df_raw[colmap["chain"]].astype(str).str.strip(),
            "site_domain": df_raw[colmap["site_domain"]].astype(str).str.strip(),
        })

        # Optional columns → canonicalize to enum
        if "retailer_class" in colmap:
            raw = df_raw[colmap["retailer_class"]].astype(str)
            out["retailer_class"] = raw.map(lambda v: canonical_class(v, default_class, class_map))
        else:
            # when missing, use the group default (already an allowed enum)
            out["retailer_class"] = default_class

        if "priority" in colmap:
            out["priority"] = (
                df_raw[colmap["priority"]].astype(str).str.lower().str.strip()
            )
            out.loc[~out["priority"].isin(["must_cover", "long_tail"]), "priority"] = "must_cover"
        else:
            out["priority"] = "must_cover"

        # Sanitize domain & website_id
        out["site_domain"] = out["site_domain"].map(to_root_domain)
        out = out[out["site_domain"] != ""]
        # Prefer ISO2; enforce 2-char uppercase when using Country
        out["country"] = out["country"].str.strip()
        out = out[out["country"].str.len() == 2]

        # Build website_id
        out["website_id"] = [
            make_website_id(str(c), str(d))
            for c, d in zip(out["country"].astype(str), out["site_domain"].astype(str))
        ]
        frames.append(out)

    df = pd.concat(frames, ignore_index=True)
    # Drop duplicates by (country, site_domain)
    df = df.drop_duplicates(subset=["country", "site_domain"])
    # Stable order for idempotency
    df = df.sort_values(["country", "chain", "site_domain"]).reset_index(drop=True)
    return df

def write_registry_yaml(df: pd.DataFrame, out_path: Path) -> None:
    rows = []
    for _, r in df.iterrows():
        rc = str(r["retailer_class"])
        if rc not in ALLOWED_CLASSES:
            # ultra-guardrail (shouldn't happen thanks to canonicalization)
            rc = "grocery"
        rows.append({
            "website_id": r["website_id"],
            "country": r["country"],
            "chain": r["chain"],
            "site_domain": r["site_domain"],
            "retailer_class": rc,
            "priority": r["priority"],
            "robots_status": "review",
            "notes": "",
        })
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml.safe_dump(rows, sort_keys=False, allow_unicode=True), encoding="utf-8")

def write_manifests(df: pd.DataFrame, manifests_dir: Path) -> None:
    manifests_dir.mkdir(parents=True, exist_ok=True)
    for iso2, chunk in df.groupby("country"):
        must = [{"website_id": wid} for wid in chunk.loc[chunk["priority"] == "must_cover", "website_id"]]
        longt = [{"website_id": wid} for wid in chunk.loc[chunk["priority"] != "must_cover", "website_id"]]
        doc = {
            "country": iso2,
            "must_cover": list(must),
            "long_tail": list(longt),
            "gates": GATES
        }
        (manifests_dir / f"{iso2}.yaml").write_text(
            yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8"
        )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grocers", required=True)
    ap.add_argument("--beauty", required=True)
    ap.add_argument("--out", required=True, help="Path to retailers/registry.yaml")
    ap.add_argument("--manifests", required=True, help="Directory for manifests/*.yaml")
    ap.add_argument("--grocers-sheet", default=None, help="Optional sheet name for grocers workbook")
    ap.add_argument("--beauty-sheet", default=None, help="Optional sheet name for beauty/pharmacy workbook")
    # Optional explicit column overrides (exact header names in the sheet)
    ap.add_argument("--country-col", default=None, help="Override column name for country/ISO2 (e.g., ISO2)")
    ap.add_argument("--chain-col", default=None, help="Override column name for chain/name (e.g., Chain/Platform)")
    ap.add_argument("--domain-col", default=None, help="Override column name for domain/url (e.g., Website)")
    # Optional class map to tweak tokens without code changes
    ap.add_argument("--class-map", default="retailers/class_map.yaml",
                    help="Optional YAML with token overrides and 'map' dict")
    args = ap.parse_args()

    class_map = load_class_map(Path(args.class_map) if args.class_map else None)

    df = load_seeds(
        Path(args.grocers),
        Path(args.beauty),
        grocers_sheet=args.grocers_sheet,
        beauty_sheet=args.beauty_sheet,
        country_col=args.country_col,
        chain_col=args.chain_col,
        domain_col=args.domain_col,
        class_map=class_map,
    )
    write_registry_yaml(df, Path(args.out))
    write_manifests(df, Path(args.manifests))
    print(f"[OK] Registry → {args.out}")
    print(f"[OK] Manifests → {args.manifests}")

if __name__ == "__main__":
    main()
