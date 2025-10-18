from __future__ import annotations

# --- repo bootstrap so eopt/ is importable even without PYTHONPATH ---
import sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[2]  # repo root
sys.path.insert(0, str(ROOT / "src"))
# ---------------------------------------------------------------------

import argparse, csv, time, random, signal
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from urllib import robotparser
import tldextract

from eopt.ids import make_website_id

UA = "Mozilla/5.0 (compatible; EOPT-Discovery/1.4)"
random.seed(42)  # determinism

# Global interrupt flag (so we can flush partial results)
INTERRUPTED = False
def _sigint(*_):
    global INTERRUPTED
    INTERRUPTED = True
signal.signal(signal.SIGINT, _sigint)

# Marketplace hints for lightweight classification
MARKETPLACES = {
    "amazon.", "ebay.", "aliexpress.", "bol.com", "allegro.", "cdiscount.",
    "zalando.", "vinted.", "rakuten."
}

def root_domain(url_or_host: str) -> str:
    host = urlparse(url_or_host).netloc or url_or_host
    ext = tldextract.extract(host)
    if not ext.domain or not ext.suffix:
        return host.lower()
    return f"{ext.domain}.{ext.suffix}".lower()

def ddg_search(query: str, max_results: int = 80) -> List[str]:
    """DuckDuckGo HTML endpoint with strict timeout."""
    try:
        url = "https://html.duckduckgo.com/html/"
        resp = requests.post(
            url,
            data={"q": query},
            headers={"User-Agent": UA},
            timeout=8,   # snappy; keeps --max-seconds effective
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        hrefs: List[str] = []
        for a in soup.select("a.result__a"):
            href = a.get("href") or ""
            if href.startswith("http"):
                hrefs.append(href)
                if len(hrefs) >= max_results:
                    break
        return hrefs
    except Exception:
        return []

def check_robots(site_root: str, timeout: float = 4.0) -> str:
    """Fetch robots.txt with a strict timeout and parse locally."""
    try:
        r = requests.get(
            f"https://{site_root}/robots.txt",
            headers={"User-Agent": UA},
            timeout=timeout,
            allow_redirects=True,
        )
        if r.status_code >= 500 or not r.text:
            return "review"
        rp = robotparser.RobotFileParser()
        rp.parse(r.text.splitlines())
        return "allowed" if rp.can_fetch(UA, "/") else "blocked"
    except requests.Timeout:
        return "review"
    except Exception:
        return "review"

def classify(host: str, title_text: str, hints: Dict[str, List[str]]) -> str:
    low = f"{host} {title_text}".lower()
    if any(mp in host for mp in MARKETPLACES):
        return "marketplace"
    for k in ("grocery","beauty","pharmacy","marketplace"):
        for w in hints.get(k, []):
            if w.lower() in low:
                return k
    if any(x in low for x in ["apotheek","pharma","pharmacie","drogerie","drugstore"]):
        return "pharmacy"
    if any(x in low for x in ["parfum","cosmétique","cosmetica","make-up","beauty"]):
        return "beauty"
    return "grocery"

def score(rd: str, title_text: str, q: str, iso2: str, cfg: Dict) -> float:
    """
    Base heuristic + boosts:
      - domain in boost_domains -> +0.35
      - TLD matches country (e.g., .nl for NL) -> +0.15
      - oil cues -> +0.35
      - supermarket cues -> +0.20
      - known retailer substrings -> +0.20
      - query term presence -> up to +~0.25 (0.05 per term)
    """
    host = rd
    low = f"{host} {title_text}".lower()
    s = 0.0

    # 1) explicit domain boosts from config
    boost_domains = {d.strip().lower() for d in cfg.get("boost_domains", [])}
    if host in boost_domains:
        s += 0.35

    # 2) country TLD match (e.g., *.nl for NL)
    tld = host.split(".")[-1]
    if tld and tld.lower() == iso2.lower():
        s += 0.15

    # 3) query-term presence
    for term in set(q.lower().split()):
        if term and term in low:
            s += 0.05

    # 4) oil cues
    if any(w in low for w in ["olive", "olijfolie", "huile d'olive", "extra vierge", "extra virgin"]):
        s += 0.35

    # 5) supermarket cues
    if any(w in low for w in ["supermarkt","supermarché","boodschappen","winkel","shop","drive","courses"]):
        s += 0.20

    # 6) known retailer substrings
    if any(x in host for x in [
        "ah","jumbo","carrefour","delhaize","colruyt","rewe","coop","spar","lidl","aldi",
        "intermarche","match","okay","bioplanet","dirk","plus","poiesz","dekamarkt","hoogvliet","picnic","cora","ekoplaza"
    ]):
        s += 0.20

    return min(1.0, round(s, 3))

def fetch_title(rd: str, timeout: float = 12.0) -> str:
    try:
        r = requests.get(
            f"https://{rd}",
            headers={"User-Agent": UA},
            timeout=timeout,
            allow_redirects=True
        )
        r.raise_for_status()
        return (BeautifulSoup(r.text, "lxml").title or "").get_text(strip=True) or rd
    except Exception:
        return rd

def add_candidate(
    candidates: List[Dict],
    seen_domains: set,
    iso2: str,
    rd: str,
    q: str,
    klass_hints: Dict[str, List[str]],
    cfg: Dict,
    fast: bool = False
) -> None:
    if not rd or rd in seen_domains:
        return
    seen_domains.add(rd)
    robots = check_robots(rd)  # bounded timeout
    title = rd if (fast or robots == "blocked") else fetch_title(rd)
    klass = classify(rd, title, klass_hints)
    sc = score(rd, title, q, iso2, cfg)
    wid = make_website_id(iso2, rd)
    candidates.append({
        "country": iso2,
        "site_domain": rd,
        "website_id": wid,
        "chain_guess": (title.split(" | ")[0] or rd)[:80],
        "retailer_class_guess": klass,
        "relevance_score": sc,
        "robots_status": robots,
        "notes": "",
    })

def write_candidates_csv(candidates: List[Dict], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "country","site_domain","website_id","chain_guess",
        "retailer_class_guess","relevance_score","robots_status","notes"
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, fieldnames=header)
        wr.writeheader()
        for r in candidates:
            wr.writerow(r)

def discover_country(
    iso2: str,
    cfg: Dict,
    out_dir: Path,
    week_tag: str,
    *,
    fast: bool = False,
    max_candidates: int | None = None,
    max_seconds: int | None = None
) -> Tuple[List[Dict], List[Tuple[str, str]]]:
    start = time.time()
    iso2 = iso2.upper()
    queries: List[str] = cfg.get("query_terms", [])
    class_hints: Dict[str, List[str]] = cfg.get("class_hints", {})
    site_filters: List[str] = cfg.get("site_filters", [f".{iso2.lower()}"])
    max_results: int = int(cfg.get("max_results", 80))
    boost_domains: List[str] = cfg.get("boost_domains", [])
    throttle_ms: int = int(cfg.get("throttle_ms", 600))  # deterministic pause

    seen_domains: set[str] = set()
    candidates: List[Dict] = []

    def maybe_stop() -> bool:
        if INTERRUPTED:
            return True
        if max_candidates and len(candidates) >= max_candidates:
            return True
        if max_seconds and (time.time() - start) >= max_seconds:
            return True
        return False

    # 1) Boosts first
    for dom in boost_domains:
        rd = root_domain(dom)
        add_candidate(candidates, seen_domains, iso2, rd, q="boost", klass_hints=class_hints, cfg=cfg, fast=fast)
        if maybe_stop():
            break

    # 2) Query × site_filter
    outer_break = False
    for q in queries:
        if outer_break or maybe_stop():
            break
        for s in site_filters:
            if maybe_stop():
                outer_break = True
                break
            qtext = f"{q} site:{s}" if s else q
            hrefs = ddg_search(qtext, max_results=max_results)
            time.sleep(throttle_ms / 1000.0)
            for url in hrefs:
                rd = root_domain(url)
                add_candidate(candidates, seen_domains, iso2, rd, q=q, klass_hints=class_hints, cfg=cfg, fast=fast)
                if maybe_stop():
                    outer_break = True
                    break
            if outer_break:
                break

    # stable order
    candidates.sort(key=lambda r: (r["site_domain"], -r["relevance_score"]))

    # write CSV regardless
    csv_path = out_dir / f"candidates_{week_tag}_{iso2}.csv"
    write_candidates_csv(candidates, csv_path)

    # hi-conf diffs (>=0.7)
    proposed = [(c["website_id"], c["chain_guess"]) for c in candidates if c["relevance_score"] >= 0.7]
    return candidates, proposed

def write_diffs(proposed: Dict[str, List[Tuple[str,str]]], out_dir: Path) -> None:
    import yaml
    reg = []
    for iso2, lst in proposed.items():
        for wid, chain in lst:
            reg.append({
                "website_id": wid,
                "country": iso2,
                "chain": chain,
                "site_domain": wid.split(":",1)[1],
                "retailer_class": "grocery",
                "priority": "long_tail",
                "robots_status": "review",
                "notes": "proposed_by_discovery",
            })
    (out_dir / "proposed_registry_diff.yaml").write_text(
        yaml.safe_dump(reg, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    diff_dir = out_dir / "proposed_manifests_diff"
    diff_dir.mkdir(parents=True, exist_ok=True)
    for iso2, lst in proposed.items():
        doc = {"country": iso2, "long_tail_add": [{"website_id": w} for w, _ in lst]}
        (diff_dir / f"{iso2}.yaml").write_text(
            yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8"
        )

def _default_week_tag() -> str:
    import datetime as dt
    y, w, _ = dt.date.today().isocalendar()
    return f"{y}-W{w:02d}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--countries", nargs="+", required=True)
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--week-tag", default=_default_week_tag())
    ap.add_argument("--fast", action="store_true",
                    help="Skip fetching titles; score by domain+query only")
    ap.add_argument("--max-candidates", type=int, default=None,
                    help="Stop after N candidates per country")
    ap.add_argument("--max-seconds", type=int, default=None,
                    help="Hard time limit per country (seconds)")
    args = ap.parse_args()

    import yaml
    cfg_all = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) or {}

    all_proposed: Dict[str, List[Tuple[str,str]]] = {}
    out_dir = Path(args.out)

    for iso2 in args.countries:
        cfg = cfg_all.get(iso2.upper(), {})
        cands, prop = discover_country(
            iso2.upper(), cfg, out_dir, args.week_tag,
            fast=args.fast, max_candidates=args.max_candidates, max_seconds=args.max_seconds
        )
        all_proposed[iso2.upper()] = prop
        print(f"[OK] {iso2.upper()}: {len(cands)} candidates "
              f"(≥0.7: {sum(1 for _,_ in prop)}){' [INTERRUPTED]' if INTERRUPTED else ''}")

    write_diffs(all_proposed, out_dir)
    print("[OK] Discovery complete.")

if __name__ == "__main__":
    main()
