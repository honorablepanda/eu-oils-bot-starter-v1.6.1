from __future__ import annotations
import re
from typing import Optional

# Accept € 3,49 ; 3.49 ; 9. 49 (with a stray space)
PRICE_RE = re.compile(r"(?i)[€\s]*([0-9]+(?:[\.,]\s*[0-9]{1,2})?)")

def parse_price_to_eur(value: str) -> Optional[float]:
    if value is None:
        return None
    s = value.replace("\xa0", " ").strip()
    # Collapse separators with stray spaces: "9. 49" -> "9.49" ; "3, 59" -> "3,59"
    s = re.sub(r"([.,])\s+(\d{1,2})", r"\1\2", s)
    m = PRICE_RE.search(s)
    if not m:
        return None
    raw = m.group(1).replace(" ", "")
    raw = raw.replace(".", "_").replace(",", ".").replace("_", "")
    try:
        f = float(raw)
    except Exception:
        return None
    if f < 0.5 or f > 200.0:
        return None
    return round(f, 2)
