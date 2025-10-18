
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

def clean_url(url: str) -> str:
    """Lowercase host, strip utm params, sort query keys, remove fragments."""
    parts = urlsplit(url)
    host = parts.netloc.lower()
    qs = [(k,v) for (k,v) in parse_qsl(parts.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    qs_sorted = sorted(qs, key=lambda kv: kv[0])
    return urlunsplit((parts.scheme, host, parts.path, urlencode(qs_sorted, doseq=True), ""))
