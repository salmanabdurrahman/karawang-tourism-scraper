"""
Shared Playwright browser lifecycle management.

Provides a context manager that owns the Playwright driver, browser, context,
and page objects, and guarantees they are closed on both success and failure.
Scrapers use this instead of managing the browser lifecycle themselves, so the
orchestration code only handles navigation, extraction, and persistence.

No browser state is shared between sessions (no parallel scraping).

Author: Salman Abdurrahman
Date: 2025
"""

from contextlib import contextmanager

from playwright.sync_api import sync_playwright


@contextmanager
def browser_session(headless=False, locale=None):
    """
    Starts Playwright, launches Chromium, and yields a page.

    The browser, context, and Playwright driver are always closed when the
    block exits, including on errors.

    Args:
        headless (bool): Run browser in headless mode.
        locale (str, optional): Locale for the browser context (e.g. "id-ID").

    Yields:
        page: Playwright page instance.
    """
    playwright = sync_playwright().start()
    browser = None
    try:
        browser = playwright.chromium.launch(headless=headless)
        if locale:
            context = browser.new_context(locale=locale)
        else:
            context = browser.new_context()
        page = context.new_page()
        yield page
    finally:
        try:
            if browser:
                browser.close()
        finally:
            playwright.stop()
