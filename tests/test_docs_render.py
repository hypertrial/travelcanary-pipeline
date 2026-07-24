import functools
import http.server
import threading
from pathlib import Path

import pytest
from playwright.sync_api import sync_playwright

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_DIR = REPO_ROOT / "site"
pytestmark = pytest.mark.repo_check


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def docs_url():
    handler = functools.partial(_QuietHandler, directory=SITE_DIR)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.fixture(scope="module")
def chromium():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            yield browser
        finally:
            browser.close()


def _new_page(chromium, viewport):
    page = chromium.new_page(viewport=viewport)
    page.route(
        "https://api.github.com/**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body='{"stargazers_count":0,"forks_count":0,"tag_name":"v0.0.0"}',
        ),
    )
    return page


def test_homepage_desktop_geometry_and_actions(chromium, docs_url):
    page = _new_page(chromium, {"width": 1440, "height": 900})
    page.goto(docs_url, wait_until="networkidle")

    assert page.locator("h1", has_text="TravelCanary").is_visible()
    assert page.locator(".tc-hero .md-button[href='getting-started/']").is_visible()
    assert page.locator(
        ".tc-hero .md-button[href='guides/query-the-warehouse/']"
    ).is_visible()
    assert page.locator(".tc-task-grid").is_visible()
    assert page.locator("a[href='audiences/analysts/']").count() >= 1
    assert page.locator("body").get_attribute("data-md-color-scheme") == "slate"


def test_homepage_mobile_task_grid(chromium, docs_url):
    page = _new_page(chromium, {"width": 390, "height": 844})
    page.goto(docs_url, wait_until="networkidle")
    assert page.locator(".tc-task-card").count() == 4
    assert page.locator(".tc-hero__mark").is_hidden()
