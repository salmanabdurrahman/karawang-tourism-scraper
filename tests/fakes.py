"""
Fake Playwright page/locator objects for extraction unit tests.

The scraper extraction functions only touch a small subset of the Playwright
API (locator(), evaluate(), inner_text, get_attribute, count, click, filter).
These fakes mirror that subset so extraction behavior can be tested without a
browser or network access. Behavior mirrors the real API where the scripts
depend on it:

- locator(selector) returns an empty locator when the selector is unknown
- .first returns the locator itself (scripts always guard with .count() > 0)
- .filter(has_text=...) keeps only elements whose text matches (regex or
  substring)
- evaluate(js, arg) records the call and returns a canned result
"""


class FakeElement:
    """One fake DOM element returned by FakeLocator.all()."""

    def __init__(self, inner_text="", attributes=None, visible=True):
        self.inner_text_value = inner_text
        self.attributes = dict(attributes or {})
        self.visible = visible

    def inner_text(self):
        return self.inner_text_value

    def get_attribute(self, name):
        return self.attributes.get(name)

    def click(self):
        pass


class FakeLocator:
    """Fake Playwright locator supporting the subset the scrapers use."""

    def __init__(self, elements=None, children=None):
        self._elements = list(elements or [])
        # Nested selector -> FakeLocator (used e.g. by the rating container)
        self._children = dict(children or {})

    @property
    def first(self):
        return self

    def count(self):
        return len(self._elements)

    def inner_text(self):
        if self._elements:
            return self._elements[0].inner_text_value
        return ""

    def get_attribute(self, name):
        if self._elements:
            return self._elements[0].attributes.get(name)
        return None

    def all_inner_texts(self):
        return [e.inner_text_value for e in self._elements]

    def all(self):
        return list(self._elements)

    def is_visible(self):
        return bool(self._elements) and self._elements[0].visible

    def click(self):
        pass

    def filter(self, has_text=None):
        if has_text is None:
            return self

        def matches(element):
            if hasattr(has_text, "search"):
                return bool(has_text.search(element.inner_text_value))
            return has_text in element.inner_text_value

        return FakeLocator([e for e in self._elements if matches(e)])

    def locator(self, selector):
        return self._children.get(selector, FakeLocator([]))


class FakePage:
    """Fake Playwright page for extraction tests."""

    def __init__(self):
        self._locators = {}
        self.evaluate_calls = []  # list of (js, arg) tuples
        self.evaluate_result = []

    def set_locator(self, selector, locator):
        """Registers the locator returned for a selector."""
        self._locators[selector] = locator

    def locator(self, selector):
        return self._locators.get(selector, FakeLocator([]))

    def evaluate(self, js, arg=None):
        self.evaluate_calls.append((js, arg))
        return self.evaluate_result

    # Stubs for the rest of the API used by orchestration code (not exercised
    # by the extraction unit tests, but kept so fake pages are swappable).
    def goto(self, url, timeout=None):
        pass

    def wait_for_selector(self, selector, timeout=None):
        pass

    def fill(self, selector, value):
        pass

    def hover(self, selector):
        pass

    def keyboard_press(self, key):
        pass

    def mouse_wheel(self, dx, dy):
        pass

    @property
    def keyboard(self):
        return self

    @property
    def mouse(self):
        return self

    def press(self, key):
        pass

    def wheel(self, dx, dy):
        pass
