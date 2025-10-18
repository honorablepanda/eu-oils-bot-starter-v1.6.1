from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Tuple


def compute_groups(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    buckets = defaultdict(list)
    for r in rows:
        buckets[(r.get("website_id"), r.get("oil"))].append(r)
    return buckets


def kpi_coverage(rows: List[Dict[str, Any]]) -> float:
    """
    % of (website_id × oil) groups that have at least ONE 'category' URL selected.
    """
    groups = compute_groups(rows)
    if not groups:
        return 0.0
    covered = 0
    for _, items in groups.items():
        if any(it.get("class") == "category" for it in items):
            covered += 1
    return 100.0 * covered / len(groups)


def kpi_stability(rows: List[Dict[str, Any]]) -> float:
    """
    Stability proxy (category-only): average months_with_snapshots across 'category' rows,
    scaled so 12 months ≈ 100%. (Capped at 100.)
    """
    cats = [int(r.get("months_with_snapshots") or 0) for r in rows if r.get("class") == "category"]
    if not cats:
        return 0.0
    avg = sum(cats) / len(cats)
    return min(100.0, (avg / 12.0) * 100.0)


def kpi_accuracy(rows: List[Dict[str, Any]]) -> float:
    """
    Accuracy proxy (URL-level only): fraction of selected rows that have oil_in_url AND locale_ok.
    """
    if not rows:
        return 0.0
    ok = 0
    for r in rows:
        s = r.get("signals", {})
        if s.get("oil_in_url") and s.get("locale_ok"):
            ok += 1
    return 100.0 * ok / len(rows)


def run_gates(rows: List[Dict[str, Any]], targets: Dict[str, float]) -> Dict[str, Any]:
    metrics = {
        "coverage": round(kpi_coverage(rows), 1),
        "stability": round(kpi_stability(rows), 1),
        "accuracy": round(kpi_accuracy(rows), 1),
    }
    results: Dict[str, Any] = {}
    all_pass = True
    for k, thresh in targets.items():
        passed = metrics.get(k, 0.0) >= float(thresh)
        results[k] = {"value": metrics.get(k, 0.0), "threshold": float(thresh), "pass": passed}
        if not passed:
            all_pass = False
    results["all_pass"] = all_pass
    return {"metrics": metrics, "results": results}
