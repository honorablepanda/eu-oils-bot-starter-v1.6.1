from __future__ import annotations
import json, random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Optional

@dataclass
class Candidate:
    id: str
    css: str
    last_score: float = 0.0
    wins_in_a_row: int = 0

@dataclass
class EvalMetrics:
    cards: int
    price_ok_rate: float
    qty_ok_rate: float
    dup_rate: float

PROMOTE_MARGIN = 0.05
REQUIRED_WINS = 2

def _policy_path(code: str) -> Path:
    p = Path("policy") / f"{code}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _load_policy(code: str) -> Dict[str, Any]:
    fp = _policy_path(code)
    if fp.exists():
        return json.loads(fp.read_text(encoding="utf-8"))
    return {"selectors": {"active": None, "candidates": []}}

def _save_policy(code: str, p: Dict[str, Any]) -> None:
    _policy_path(code).write_text(json.dumps(p, ensure_ascii=False, indent=2), encoding="utf-8")

def _explain_path(run_dir: Path) -> Path:
    return run_dir / "selectors_explained.json"

def shadow_score(m: EvalMetrics) -> float:
    return 0.5 * m.price_ok_rate + 0.5 * m.qty_ok_rate

def _gates_pass(m: EvalMetrics) -> bool:
    return (m.cards >= 5 and m.price_ok_rate >= 0.70 and m.qty_ok_rate >= 0.50 and m.dup_rate <= 0.20)

def maybe_promote(code: str, run_dir: Path, baseline: Candidate, challengers: List[Candidate], baseline_metrics: EvalMetrics, challenger_metrics: Dict[str, EvalMetrics]) -> Candidate:
    """
    Returns the selected candidate (either baseline or a promoted challenger).
    Rules:
      - Challenger must outscore baseline by >= PROMOTE_MARGIN *and* pass gates.
      - Needs 2 consecutive wins to promote.
      - First regression → auto rollback to baseline.
    """
    policy = _load_policy(code)
    active_id = policy["selectors"].get("active") or baseline.id

    # Update candidates registry
    reg: Dict[str, Dict[str, Any]] = {baseline.id: {"css": baseline.css, "wins": 0}}
    for c in challengers:
        reg[c.id] = {"css": c.css, "wins": 0}
    policy["selectors"]["candidates"] = reg

    base_score = shadow_score(baseline_metrics)
    winner = baseline
    promoted = False

    # Identify best challenger
    best: Optional[Candidate] = None
    best_score = base_score
    for c in challengers:
        m = challenger_metrics.get(c.id)
        if not m:
            continue
        s = shadow_score(m)
        if s > best_score and (s - base_score) >= PROMOTE_MARGIN and _gates_pass(m):
            best = c
            best_score = s

    # Apply consecutive win rule
    if best is not None:
        # increment its wins
        wins = policy["selectors"]["candidates"][best.id].get("wins", 0) + 1
        policy["selectors"]["candidates"][best.id]["wins"] = wins
        if wins >= REQUIRED_WINS:
            policy["selectors"]["active"] = best.id
            winner = best
            promoted = True
            # reset others' wins
            for k in list(policy["selectors"]["candidates"].keys()):
                if k != best.id:
                    policy["selectors"]["candidates"][k]["wins"] = 0
    else:
        # No challenger beat baseline this round ⇒ record a regression for non-winners
        for k in list(policy["selectors"]["candidates"].keys()):
            if k != baseline.id:
                policy["selectors"]["candidates"][k]["wins"] = 0
        policy["selectors"]["active"] = baseline.id
        winner = baseline

    # Save explanation for audit
    explanation = {
        "active_before": active_id,
        "baseline": {"id": baseline.id, "score": round(base_score, 3)},
        "challengers": {cid: {"score": round(shadow_score(challenger_metrics[cid]), 3) if cid in challenger_metrics else None,
                              "wins": policy["selectors"]["candidates"][cid]["wins"]} for cid in policy["selectors"]["candidates"] if cid != baseline.id},
        "promoted": promoted,
        "active_after": policy["selectors"]["active"],
        "gates": {
            "cards>=5": baseline_metrics.cards >= 5,
            "price>=70%": baseline_metrics.price_ok_rate >= 0.70,
            "qty>=50%": baseline_metrics.qty_ok_rate >= 0.50,
            "dup<=20%": baseline_metrics.dup_rate <= 0.20,
        }
    }
    _explain_path(run_dir).write_text(json.dumps(explanation, ensure_ascii=False, indent=2), encoding="utf-8")
    _save_policy(code, policy)
    return winner
