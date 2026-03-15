from pathlib import Path
from playwright.sync_api import sync_playwright

SESSION_DIR = Path("data/sessions")
SESSION_DIR.mkdir(parents=True, exist_ok=True)

STORAGE_STATE_PATH = SESSION_DIR / "facebook_state.json"


def save_facebook_login_state() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)
        context = browser.new_context()
        page = context.new_page()

        page.goto("https://www.facebook.com/", wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

        print("\nBrowser opened.")
        print("1. Log into Facebook manually")
        print("2. Open Marketplace manually and confirm it works")
        print("3. Return to terminal and press Enter")
        input("Press Enter to save session state... ")

        context.storage_state(path=str(STORAGE_STATE_PATH))
        print(f"[OK] Saved Facebook session to: {STORAGE_STATE_PATH}")

        browser.close()


if __name__ == "__main__":
    save_facebook_login_state()