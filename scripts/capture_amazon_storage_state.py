from __future__ import annotations

import argparse
import asyncio
import base64
from pathlib import Path

from playwright.async_api import async_playwright


async def _capture_state(domain: str, output: Path) -> str:
    output.parent.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False, slow_mo=120)
        context = await browser.new_context(locale="it-IT")
        page = await context.new_page()
        try:
            await page.goto(f"https://{domain}", wait_until="domcontentloaded")
            print(f"Browser opened on https://{domain}")
            print("1) Accedi con il tuo account Amazon.")
            print("2) Risolvi eventuale 2FA/captcha.")
            print("3) Apri almeno una pagina risultati ricerca Amazon.")
            await asyncio.to_thread(input, "Premi INVIO qui quando hai finito: ")

            await context.storage_state(path=str(output))
            raw = output.read_bytes()
            encoded = base64.b64encode(raw).decode("ascii")
            return encoded
        finally:
            await context.close()
            await browser.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture Amazon Playwright storage state and print base64.")
    parser.add_argument(
        "--domain",
        default="www.amazon.it",
        help="Amazon domain to authenticate (default: www.amazon.it).",
    )
    parser.add_argument(
        "--output",
        default=".tmp/amazon_storage_state.json",
        help="Path to storage_state JSON output.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    output = Path(args.output).expanduser().resolve()
    encoded = asyncio.run(_capture_state(args.domain, output))
    print("\nStorage state saved to:", output)
    print("\nSet this as GitHub Secret AMAZON_WAREHOUSE_STORAGE_STATE_B64:\n")
    print(encoded)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
