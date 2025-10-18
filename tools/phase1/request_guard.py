#!/usr/bin/env python3
# tools/phase1/request_guard.py
from __future__ import annotations
from urllib.parse import urlparse, parse_qsl

ALLOWED = {
    # Netherlands
    "ah.nl": {
        "paths": ["/producten/"],                  # category only
        "allow_params": {"page", "withOffset"},    # AH wants ?page=&withOffset=true
        "deny_patterns": ["/zoeken"],              # no search scraping
    },
    "jumbo.com": {
        "paths": ["/"],                            # allow categories under root
        "allow_params": {"offSet"},                # Jumbo whitelists ?offSet=
        "deny_params": {"page","q","search","sort","filter"},
    },
    # Belgium
    "colruyt.be": {
        "paths": ["/"],                            # categories OK
        "deny_paths": ["/producten/product-detail"],  # PDP disallowed
    },
    "carrefour.be": {
        "paths": ["/"],                            # categories; we’ll rely on scroll/load-more
        # keep flexible on params; rely on live behavior + consent
    },
}

def guard_url(url: str, retailer_code: str|None = None) -> tuple[bool,str]:
    """
    Returns (allowed, reason). We enforce path & query rules where we know them.
    If domain isn't in ALLOWED, we allow but tag 'review'.
    """
    try:
        u = urlparse(url)
        host = u.netloc.lower()
        domain = ".".join(host.split(".")[-2:]) if "." in host else host
        rules = ALLOWED.get(domain)
        if not rules:
            return True, f"review: no rules for {domain}"

        # path rules
        path = (u.path or "/")
        if "deny_paths" in rules and any(path.startswith(p) for p in rules["deny_paths"]):
            return False, f"deny: path {path} denied for {domain}"
        if "paths" in rules and not any(path.startswith(p) for p in rules["paths"]):
            return False, f"deny: path {path} not in allowlist for {domain}"

        # query rules
        qs = dict(parse_qsl(u.query, keep_blank_values=True))
        if "allow_params" in rules:
            # only allow these; ignore if no query
            disallowed = set(qs.keys()) - set(rules["allow_params"])
            if qs and disallowed:
                return False, f"deny: params {sorted(disallowed)} not allowed for {domain}"
        if "deny_params" in rules:
            denied = set(qs.keys()) & set(rules["deny_params"])
            if denied:
                return False, f"deny: params {sorted(denied)} forbidden for {domain}"

        # AH needs withOffset=true when page is used
        if domain == "ah.nl":
            if "page" in qs and qs.get("withOffset") != "true":
                return False, "deny: ah.nl requires withOffset=true with page"
        return True, "allowed"
    except Exception as e:
        return False, f"error: {e}"

def guard_or_raise(url: str, retailer_code: str|None = None):
    ok, reason = guard_url(url, retailer_code)
    if not ok:
        raise RuntimeError(f"[ROBOTS_GUARD] {reason} :: {url}")
    return reason
