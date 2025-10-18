#!/usr/bin/env python
from __future__ import annotations

import argparse, json, sys, csv, os
from pathlib import Path
from typing import List, Dict, Any

try:
    import yaml
except Exception:
    yaml = None

from finder.core.canonicalize import clean_url
from finder.core.generate_candidates import generate_candidates_for_retailer_oil
from finder.core.score_select import score_candidate, select_per_group
from finder.core.evidence import Evidence
from finder.core.reports import write_finder_report
from finder.core.enrich import run as enrich_run
from finder.core.gates import run_gates

DATA_DIR = Path(__file__).resolve().parent / 'data'

def load_yaml(path: Path) -> dict:
    if yaml:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    d = {}
    cur = None
    for line in path.read_text(encoding='utf-8').splitlines():
        if not line.strip() or line.strip().startswith('#'):
            continue
        if ':' in line and not line.lstrip().startswith('-'):
            key = line.split(':')[0].strip()
            cur = key
            d[cur] = []
        elif line.strip().startswith('-') and cur:
            d[cur].append(line.split('-',1)[1].strip().strip('\"'))
    return d

def load_retailers(csv_path: Path) -> List[Dict[str, str]]:
    with open(csv_path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def cmd_discover(args):
    products = load_yaml(Path(args.products))
    keywords = load_yaml(Path(args.keywords))

    for iso in args.countries:
        out_all = []
        ret_path = Path(f'retailers/retailers_{iso}.csv')
        if not ret_path.exists():
            print(f'[WARN] Missing {ret_path} — skipping {iso}', file=sys.stderr)
            continue
        for r in load_retailers(ret_path):
            for oil in products.get('oils', []):
                cands = generate_candidates_for_retailer_oil(r, oil, keywords)
                out_all.extend(cands)

        out_file = DATA_DIR / f'candidates_{iso}.json'
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps(out_all, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'[OK] Wrote {out_file} ({len(out_all)} candidates)')

def cmd_enrich(args):
    for iso in args.countries:
        src = DATA_DIR / f'candidates_{iso}.json'
        if not src.exists():
            print(f'[WARN] No candidates for {iso} at {src}', file=sys.stderr)
            continue
        dst = DATA_DIR / f'candidates_enriched_{iso}.json'
        enrich_run(
            input_path=src,
            output_path=dst,
            products_path=Path(args.products),
            keywords_path=Path(args.keywords),
            locales_path=Path(args.locales),
            start_year=args.start_year,
        )
        print(f'[OK] Enriched → {dst}')

def _read_candidates(iso: str) -> List[Dict[str,Any]]:
    enriched = DATA_DIR / f'candidates_enriched_{iso}.json'
    plain = DATA_DIR / f'candidates_{iso}.json'
    p = enriched if enriched.exists() else plain
    if not p.exists():
        return []
    return json.loads(p.read_text(encoding='utf-8'))

def cmd_select(args):
    for iso in args.countries:
        cands = _read_candidates(iso)
        if not cands:
            print(f'[WARN] No candidate file for {iso}', file=sys.stderr)
            continue
        for c in cands:
            c['score'] = score_candidate(c.get('signals', {}), c)
        selected = select_per_group(cands, group_keys=['website_id','oil','class'], k=args.per_group)
        (DATA_DIR / f'selected_{iso}.json').write_text(json.dumps(selected, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'[OK] Wrote selected for {iso}: {len(selected)} rows')

def cmd_report(args):
    all_rows = []
    for iso in args.countries:
        sel_path = DATA_DIR / f'selected_{iso}.json'
        if not sel_path.exists():
            print(f'[WARN] No selected for {iso} at {sel_path}', file=sys.stderr)
            continue
        rows = json.loads(sel_path.read_text(encoding='utf-8'))
        for r in rows:
            r['_iso'] = iso
        all_rows.extend(rows)
    Path('reports').mkdir(parents=True, exist_ok=True)
    out_xlsx = Path('reports') / 'finder_report_ALL.xlsx'
    write_finder_report(all_rows, out_xlsx)
    print(f'[OK] Report → {out_xlsx}')

def cmd_gate(args):
    all_rows = []
    for iso in args.countries:
        sel_path = DATA_DIR / f'selected_{iso}.json'
        if not sel_path.exists():
            print(f'[WARN] No selected for {iso} at {sel_path}', file=sys.stderr)
            continue
        rows = json.loads(sel_path.read_text(encoding='utf-8'))
        all_rows.extend(rows)

    targets = {'accuracy': args.accuracy, 'stability': args.stability, 'coverage': args.coverage}
    out = run_gates(all_rows, targets)
    print('[KPIs]', out['metrics'])
    for k, v in out['results'].items():
        if k == 'all_pass': continue
        print(f'  - {k}: {v["value"]} / {v["threshold"]} → {"PASS" if v["pass"] else "FAIL"}')
    if out['results'].get('all_pass', True) and all(v['pass'] for k,v in out['results'].items() if k != 'all_pass'):
        print('[PASS] Finder gates met.')
        raise SystemExit(0)
    else:
        print('[FAIL] Finder gates not met.')
        raise SystemExit(2)

def build_parser():
    p = argparse.ArgumentParser(prog='finder')
    sub = p.add_subparsers(dest='cmd', required=True)

    d = sub.add_parser('discover', help='Generate candidates (archive-first, slug/search/sitemaps).')
    d.add_argument('--countries', nargs='+', required=True)
    d.add_argument('--products', default='config/products.yml')
    d.add_argument('--keywords', default='config/keywords.yml')
    d.set_defaults(func=cmd_discover)

    e = sub.add_parser('enrich', help='Enrich candidates with archive/CDX & URL-level signals.')
    e.add_argument('--countries', nargs='+', required=True)
    e.add_argument('--products', default='config/products.yml')
    e.add_argument('--keywords', default='config/keywords.yml')
    e.add_argument('--locales', default='config/locales.yml')
    e.add_argument('--start-year', type=int, default=2022)
    e.set_defaults(func=cmd_enrich)

    s = sub.add_parser('select', help='Score and select best per website×oil×class.')
    s.add_argument('--countries', nargs='+', required=True)
    s.add_argument('--per-group', type=int, default=2)
    s.set_defaults(func=cmd_select)

    r = sub.add_parser('report', help='Emit Excel coverage/quality report.')
    r.add_argument('--countries', nargs='+', required=True)
    r.set_defaults(func=cmd_report)

    g = sub.add_parser('gate', help='Run KPIs/gates (real metrics).')
    g.add_argument('--countries', nargs='+', required=True)
    g.add_argument('--accuracy', type=float, default=95.0)
    g.add_argument('--stability', type=float, default=80.0)
    g.add_argument('--coverage', type=float, default=70.0)
    g.set_defaults(func=cmd_gate)

    return p

def main(argv=None):
    parser = build_parser()
    ns = parser.parse_args(argv)
    ns.func(ns)

if __name__ == '__main__':
    main()
