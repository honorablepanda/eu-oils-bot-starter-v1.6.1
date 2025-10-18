from __future__ import annotations
from playwright.sync_api import Page

def detect_auth_redirect(page: Page) -> bool:
    url = page.url.lower()
    needles = ["/login", "/signin", "/identity", "auth."]
    return any(n in url for n in needles)

def detect_cf_challenge(page: Page) -> bool:
    # Heuristic: common Cloudflare challenge markers
    texts = ["Checking your browser", "Verify you are human", "cloudflare"]  # rough
    body = page.content().lower()
    if any(t.lower() in body for t in texts):
        return True
    # iframe marker
    iframes = page.query_selector_all("iframe[src*='challenges.cloudflare.com']")
    return len(iframes) > 0

def detect_cookie_wall(page: Page) -> bool:
    # Look for common cookie banners that hijack interaction
    selectors = [
        "#onetrust-accept-btn-handler",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "button:has-text('Alles accepteren')",
        "button:has-text('Accepter')",
        "button:has-text('Accepteren')",
    ]
    for sel in selectors:
        el = page.query_selector(sel)
        if el is not None:
            return True
    return False

def detect_empty_listing(card_count: int) -> bool:
    return card_count < 5
