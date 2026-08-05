"""
Unit tests for the shared Playwright browser lifecycle helper.

browser_session is exercised with a fake Playwright driver (no real browser
launched), covering the happy path, locale propagation, and guaranteed
cleanup when the block raises or the browser fails to launch.
"""

import os
import sys
import unittest
from unittest import mock

_HERE = os.path.dirname(os.path.abspath(__file__))
for _path in (os.path.join(_HERE, ".."), os.path.join(_HERE, "..", "src")):
    _path = os.path.abspath(_path)
    if _path not in sys.path:
        sys.path.insert(0, _path)
del _HERE, _path

import browser


class FakePage:
    pass


class FakeContext:
    def __init__(self):
        self.page = FakePage()

    def new_page(self):
        return self.page


class FakeBrowser:
    def __init__(self):
        self.context = FakeContext()
        self.closed = False
        self.last_context_kwargs = None

    def new_context(self, **kwargs):
        self.last_context_kwargs = kwargs
        return self.context

    def close(self):
        self.closed = True


class FakeChromium:
    def __init__(self):
        self.browser = FakeBrowser()
        self.launch_kwargs = None

    def launch(self, **kwargs):
        self.launch_kwargs = kwargs
        return self.browser


class FakePlaywright:
    def __init__(self):
        self.chromium = FakeChromium()
        self.stopped = False

    def start(self):
        return self

    def stop(self):
        self.stopped = True


class BrowserSessionTest(unittest.TestCase):
    def setUp(self):
        self.fake = FakePlaywright()
        self.patcher = mock.patch("browser.sync_playwright", return_value=self.fake)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def test_yields_page_and_closes_everything(self):
        with browser.browser_session(headless=True) as page:
            self.assertIs(page, self.fake.chromium.browser.context.page)
        self.assertEqual(self.fake.chromium.launch_kwargs, {"headless": True})
        self.assertTrue(self.fake.chromium.browser.closed)
        self.assertTrue(self.fake.stopped)

    def test_locale_passed_to_context(self):
        with browser.browser_session(locale="id-ID"):
            pass
        self.assertEqual(
            self.fake.chromium.browser.last_context_kwargs, {"locale": "id-ID"}
        )

    def test_no_locale_uses_default_context(self):
        with browser.browser_session():
            pass
        self.assertEqual(self.fake.chromium.browser.last_context_kwargs, {})

    def test_closes_on_error_inside_block(self):
        with self.assertRaises(RuntimeError):
            with browser.browser_session():
                raise RuntimeError("boom")
        self.assertTrue(self.fake.chromium.browser.closed)
        self.assertTrue(self.fake.stopped)

    def test_stop_called_when_launch_fails(self):
        self.fake.chromium.launch = mock.Mock(side_effect=RuntimeError("no browser"))
        with self.assertRaises(RuntimeError):
            with browser.browser_session():
                pass
        self.assertFalse(self.fake.chromium.browser.closed)
        self.assertTrue(self.fake.stopped)


if __name__ == "__main__":
    unittest.main()
