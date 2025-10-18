from __future__ import annotations

def _digits(s: str) -> str:
    return ''.join(ch for ch in s if ch.isdigit())

def is_valid_gtin(code: str) -> bool:
    s = _digits(code)
    if len(s) not in (8, 12, 13, 14):
        return False
    s = s.zfill(14)
    total = 0
    for i, ch in enumerate(s[:-1]):
        n = ord(ch) - 48
        weight = 3 if (13 - i) % 2 == 0 else 1
        total += n * weight
    check = (10 - (total % 10)) % 10
    return check == (ord(s[-1]) - 48)
