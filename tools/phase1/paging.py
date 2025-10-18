#!/usr/bin/env python3
# tools/phase1/paging.py
from __future__ import annotations
from typing import Iterable

class PagerTracker:
    def __init__(self, min_growth: int = 1, max_stalls: int = 1):
        self.seen = set()
        self.stalls = 0
        self.min_growth = min_growth
        self.max_stalls = max_stalls

    def add_batch(self, product_keys: Iterable[str]) -> bool:
        """
        Add a batch of product identity keys (e.g., hash(url|name|size)).
        Returns True if growth >= min_growth; False if stall.
        """
        before = len(self.seen)
        for k in product_keys:
            if k: self.seen.add(k)
        growth = len(self.seen) - before
        if growth < self.min_growth:
            self.stalls += 1
            return False
        self.stalls = 0
        return True

    def should_stop(self) -> bool:
        return self.stalls >= self.max_stalls
