
from __future__ import annotations
from dataclasses import dataclass, asdict
import json

@dataclass
class Evidence:
    website_id: str
    oil: str
    url: str
    score: int
    locks_passed: list
    evidence: dict

    def to_jsonl(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)
