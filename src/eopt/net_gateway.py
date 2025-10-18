from __future__ import annotations
import time, threading, urllib.parse, urllib.robotparser as rp
from dataclasses import dataclass
import httpx

@dataclass
class GatewayPolicy:
    user_agent: str = "EOPT/1.0 (+compliance; polite)"
    rps: float = 0.7
    burst: int = 4
    timeout_s: float = 20.0
    budget_s: float = 120.0
    respect_robots: bool = True

class _Bucket:
    def __init__(self, rps: float, burst: int):
        self.capacity = burst
        self.tokens = burst
        self.rps = rps
        self.last = time.monotonic()
        self.lock = threading.Lock()
    def take(self, n=1):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last)*self.rps)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return True
            return False

class NetGateway:
    def __init__(self, policy: GatewayPolicy):
        self.p = policy
        self.bucket = _Bucket(policy.rps, policy.burst)
        self._robots_cache: dict[str, rp.RobotFileParser] = {}
        self._client = httpx.Client(timeout=policy.timeout_s, follow_redirects=True,
                                    headers={"User-Agent": policy.user_agent})

    def _robots_allowed(self, url: str) -> bool:
        if not self.p.respect_robots:
            return True
        parts = urllib.parse.urlparse(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        r = self._robots_cache.get(origin)
        if not r:
            r = rp.RobotFileParser()
            r.set_url(urllib.parse.urljoin(origin, "/robots.txt"))
            try:
                r.read()
            except Exception:
                pass  # default to cautious allow if robots is unreachable
            self._robots_cache[origin] = r
        return r.can_fetch(self.p.user_agent, url)

    def get_text(self, url: str, *, budget_s: float | None = None, referer: str | None = None):
        start = time.monotonic()
        bud = budget_s or self.p.budget_s
        meta = {"url": url, "ok": False, "status": None, "why": None, "elapsed_s": None}
        if not self._robots_allowed(url):
            meta["why"] = "robots_disallow"
            return None, meta
        while time.monotonic() - start < bud:
            if self.bucket.take():
                try:
                    headers = {}
                    if referer:
                        headers["Referer"] = referer
                    r = self._client.get(url, headers=headers)
                    meta["status"] = r.status_code
                    if r.status_code == 200:
                        meta["ok"] = True
                        meta["elapsed_s"] = round(time.monotonic() - start, 3)
                        return r.text, meta
                    if r.status_code in (403, 429):
                        meta["why"] = f"http_{r.status_code}"
                        break
                except httpx.HTTPError as e:
                    meta["why"] = f"http_error:{type(e).__name__}"
            time.sleep(0.3)
        if meta["elapsed_s"] is None:
            meta["elapsed_s"] = round(time.monotonic() - start, 3)
        if meta["why"] is None:
            meta["why"] = "budget_exhausted"
        return None, meta
