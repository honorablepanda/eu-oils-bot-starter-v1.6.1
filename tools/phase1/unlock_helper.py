from playwright.sync_api import sync_playwright
import sys, time, pathlib

profile_dir, url, done_file = sys.argv[1], sys.argv[2], sys.argv[3]
p = sync_playwright().start()
try:
    browser = p.chromium.launch_persistent_context(profile_dir, headless=False)
    page = browser.new_page()
    page.goto(url, wait_until="load", timeout=45000)
    print("[UNLOCK] Complete any checks, then close this window.")
    while any(not t.is_closed() for t in browser.pages):
        time.sleep(1)
finally:
    p.stop()
pathlib.Path(done_file).write_text("ok")
