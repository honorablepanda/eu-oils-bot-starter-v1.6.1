# tools/phase1/utils_playwright.py
from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Tuple

from playwright.sync_api import (
    Browser,
    BrowserContext,
    sync_playwright,
)


# Where each retailer keeps its persistent Chrome profile (cookies, store picks, etc.)
def _profile_dir(retailer_code: str) -> Path:
    p = Path("_pw_profile") / retailer_code
    p.mkdir(parents=True, exist_ok=True)
    return p


def open_persistent_context(
    retailer_code: str,
    headless: bool = True,
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) EOPT/1.0',
    locale: str = "nl-NL",
    timezone_id: str = "Europe/Amsterdam",
    width: int = 1366,
    height: int = 768,
) -> Tuple[Optional[Browser], BrowserContext, Callable[[], None]]:
    """
    Launch a persistent Chromium context so cookies / consent / store selection stick
    per retailer. Returns (browser, context, shutdown).
    """
    profile = _profile_dir(retailer_code)

    # keep Playwright alive until caller calls shutdown()
    p = sync_playwright().start()

    ctx = p.chromium.launch_persistent_context(
        user_data_dir=str(profile),
        headless=headless,
        locale=locale,
        timezone_id=timezone_id,
        user_agent=user_agent,
        viewport={"width": width, "height": height},
        ignore_https_errors=True,
        # Be a little quieter with default permissions; most sites work fine with defaults.
    )

    # Polite defaults for all pages created in this context
    try:
        ctx.set_extra_http_headers(
            {
                "DNT": "1",
                # A realistic referrer that matches our playbook entry path
                "Referer": "https://duckduckgo.com/?q=olijfolie",
            }
        )
    except Exception:
        pass

    def _shutdown() -> None:
        try:
            ctx.close()
        except Exception:
            pass
        try:
            p.stop()
        except Exception:
            pass

    # In persistent mode, ctx.browser is set; keep signature consistent with callers
    return (ctx.browser, ctx, _shutdown)


def operator_unlock(
    retailer_code: str,
    target_url: str,
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) EOPT/1.0',
    locale: str = "nl-NL",
    timezone_id: str = "Europe/Amsterdam",
    width: int = 1366,
    height: int = 768,
) -> None:
    """
    Open a HEADED window with the same persistent profile so an operator
    can accept cookies / solve challenges once. We do not loop here;
    the caller is expected to throttle calls to this helper.
    """
    profile = _profile_dir(retailer_code)
    p = sync_playwright().start()
    ctx = None
    try:
        ctx = p.chromium.launch_persistent_context(
            user_data_dir=str(profile),
            headless=False,  # headed for human interaction
            locale=locale,
            timezone_id=timezone_id,
            user_agent=user_agent,
            viewport={"width": width, "height": height},
            ignore_https_errors=True,
        )

        try:
            ctx.set_extra_http_headers(
                {
                    "DNT": "1",
                    "Referer": "https://duckduckgo.com/?q=olijfolie",
                }
            )
        except Exception:
            pass

        page = ctx.new_page()
        try:
            page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
        except Exception:
            # still let the operator see the window
            pass

        print("\n[UNLOCK] A browser window opened using the same persistent profile.")
        print("[UNLOCK] Please accept cookies and complete any human/verification prompts.")
        print("[UNLOCK] When finished, return to this console and press ENTER to continue.\n")

        # Block until the operator confirms
        try:
            input("[UNLOCK] Press ENTER here when done…")
        except EOFError:
            # In non-interactive shells just wait a moment
            page.wait_for_timeout(1500)

    finally:
        try:
            if ctx:
                ctx.close()
        except Exception:
            pass
        try:
            p.stop()
        except Exception:
            pass
