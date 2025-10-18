# tools/phase1/store_context.py
from __future__ import annotations
import asyncio

async def ensure_store_selected(page, preferred_store_name: str,
                               open_sel: str = ".store-picker",
                               confirm_sel: str = ".confirm-store",
                               timeout_open=4000, timeout_click=4000) -> bool:
    """
    Open the store picker, select preferred store by visible text, confirm, and persist.
    Returns True if it changed selection.
    """
    changed = False
    try:
        btn = await page.wait_for_selector(open_sel, timeout=timeout_open)
        if btn:
            await btn.click()
            await page.wait_for_timeout(300)
            # select store by text
            el = await page.wait_for_selector(f"text={preferred_store_name}", timeout=5000)
            if el:
                await el.click()
                await page.wait_for_timeout(300)
                # confirm
                c = await page.wait_for_selector(confirm_sel, timeout=timeout_click)
                if c:
                    await c.click()
                    await page.wait_for_timeout(800)
                    changed = True
    except Exception:
        pass
    return changed
