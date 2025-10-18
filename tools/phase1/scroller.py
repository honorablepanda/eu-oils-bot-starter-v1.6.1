from __future__ import annotations
import random
from typing import Optional, Iterable
from playwright.sync_api import Page


def try_accept_cookies(page: Page) -> None:
    selectors = [
        "#onetrust-accept-btn-handler",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "#CybotCookiebotDialogBodyButtonAccept",
        "button:has-text('Alles accepteren')",
        "button:has-text('Accepteren')",
        "button:has-text('Accepter')",
        "button[aria-label*='accept' i]",
        "button[aria-label*='akkoord' i]",
    ]
    for sel in selectors:
        try:
            if page.query_selector(sel):
                page.click(sel, timeout=3000)
                page.wait_for_timeout(500)
                break
        except Exception:
            pass


# --- upgraded, broader CMP handling (page + iframes) ---
_COOKIE_TEXTS: Iterable[str] = (
    # NL/BE/FR/EN common strings
    "Alles accepteren", "Alle cookies", "Ik ga akkoord", "Accepteren", "Akkoord",
    "Tout accepter", "Accepter", "J'accepte",
    "Accept all", "Accept", "Agree", "Allow all", "Allow",
)
_COOKIE_SELECTORS: Iterable[str] = (
    "#onetrust-accept-btn-handler",
    "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
    "#CybotCookiebotDialogBodyButtonAccept",
    ".ot-sdk-container #onetrust-accept-btn-handler",
    "[data-test='accept-all']",
    ".cookie-accept, .consent-accept, .btn-accept",
    "button[class*='accept' i], a[class*='accept' i]",
)


def _click_any(page_like, selectors: Iterable[str]) -> bool:
    for sel in selectors:
        try:
            loc = page_like.locator(sel)
            if loc.count() == 0:
                continue
            # click first visible
            for i in range(min(4, loc.count())):
                el = loc.nth(i)
                try:
                    if el.is_visible():
                        el.click(timeout=1000)
                        return True
                except Exception:
                    continue
            # fallback: first element
            try:
                loc.first.click(timeout=1000)
                return True
            except Exception:
                pass
        except Exception:
            continue
    return False


def aggressive_accept_cookies(page: Page, wait_after_ms: int = 800) -> bool:
    """Try multiple strategies on page and within iframes; return True if we clicked something."""
    clicked = False
    if _click_any(page, _COOKIE_SELECTORS):
        clicked = True
    if not clicked:
        text_sels = [f"button:has-text('{t}')" for t in _COOKIE_TEXTS] + [f"a:has-text('{t}')" for t in _COOKIE_TEXTS]
        if _click_any(page, text_sels):
            clicked = True
    if not clicked:
        for fr in page.frames:
            try:
                if _click_any(fr, _COOKIE_SELECTORS):
                    clicked = True
                    break
                text_sels = [f"button:has-text('{t}')" for t in _COOKIE_TEXTS] + [f"a:has-text('{t}')" for t in _COOKIE_TEXTS]
                if _click_any(fr, text_sels):
                    clicked = True
                    break
            except Exception:
                continue
    if clicked:
        try:
            page.wait_for_timeout(wait_after_ms)
        except Exception:
            pass
    return clicked


def bounded_scroll(page: Page, max_steps: int = 10, sleep_min: float = 0.3, sleep_max: float = 1.0) -> None:
    for _ in range(max_steps):
        page.mouse.wheel(0, 2000)
        page.wait_for_timeout(int(random.uniform(sleep_min, sleep_max) * 1000))


def collect_card_count(page: Page, candidate_selectors: list[str]) -> int:
    for sel in candidate_selectors:
        try:
            els = page.query_selector_all(sel)
            if len(els) >= 1:
                return len(els)
        except Exception:
            continue
    return 0


def click_load_more(page: Page, load_more_selector: Optional[str], max_pages: int) -> None:
    if not load_more_selector:
        return
    pages = 0
    while pages < max_pages:
        btn = page.query_selector(load_more_selector)
        if not btn:
            break
        try:
            btn.click()
            page.wait_for_load_state("networkidle")
        except Exception:
            break
        pages += 1
