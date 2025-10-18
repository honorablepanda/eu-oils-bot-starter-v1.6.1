from __future__ import annotations
import re
from typing import Optional

# Support ml | l | ltr | liter | litre (case-insensitive)
UNIT = r"(ml|l|ltr|liter|litre)"
PATTERNS = [
    re.compile(rf"(?i)(\d+)\s*[x×]\s*(\d+(?:[.,]\d+)?)\s*{UNIT}"),
    re.compile(rf"(?i)(\d+(?:[.,]\d+)?)\s*{UNIT}"),
]

def parse_quantity(s: str) -> Optional[str]:
    if not s:
        return None
    ss = s.replace("\xa0", " ").strip()
    for rx in PATTERNS:
        m = rx.search(ss)
        if not m:
            continue
        if rx is PATTERNS[0]:
            packs = int(m.group(1))
            amt = m.group(2).replace(",", ".")
            unit = m.group(3).lower()
            return f"{packs}x{amt} {unit.upper()}"
        else:
            amt = m.group(1).replace(",", ".")
            unit = m.group(2).lower()
            return f"{amt} {unit.upper()}"
    return None
