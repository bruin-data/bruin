"""
Pytest configuration and fixtures for User & Team Management e2e tests.

This module provides Playwright fixtures for browser-based testing.
"""

import os

import pytest
from playwright.sync_api import Page, sync_playwright


@pytest.fixture(scope="session")
def base_url() -> str:
    """Get the base URL for the application under test.
    
    Returns the BASE_URL environment variable or defaults to localhost.
    """
    return os.environ.get("BASE_URL", "http://localhost:3000")


@pytest.fixture(scope="session")
def browser_context_args():
    """Configure browser context options."""
    return {
        "viewport": {"width": 1280, "height": 720},
        "ignore_https_errors": True,
    }


@pytest.fixture(scope="session")
def playwright_instance():
    """Create a Playwright instance for the test session."""
    with sync_playwright() as playwright:
        yield playwright


@pytest.fixture(scope="session")
def browser(playwright_instance):
    """Launch a browser instance for the test session."""
    browser = playwright_instance.chromium.launch(
        headless=os.environ.get("HEADLESS", "true").lower() == "true"
    )
    yield browser
    browser.close()


@pytest.fixture
def context(browser, browser_context_args):
    """Create a new browser context for each test."""
    context = browser.new_context(**browser_context_args)
    yield context
    context.close()


@pytest.fixture
def page(context) -> Page:
    """Create a new page for each test."""
    page = context.new_page()
    yield page
    page.close()


@pytest.fixture
def authenticated_page(page: Page, base_url: str) -> Page:
    """Provide an authenticated page for tests requiring login.
    
    This fixture handles authentication before returning the page.
    Configure authentication via environment variables:
    - TEST_USER_EMAIL: User email for login
    - TEST_USER_PASSWORD: User password for login
    """
    email = os.environ.get("TEST_USER_EMAIL", "test@example.com")
    password = os.environ.get("TEST_USER_PASSWORD", "testpassword")
    
    # Navigate to login page and authenticate
    page.goto(f"{base_url}/login")
    
    # Fill in login credentials
    page.fill('input[name="email"], input[type="email"], #email', email)
    page.fill('input[name="password"], input[type="password"], #password', password)
    
    # Submit the login form
    page.click('button[type="submit"], #login-button, button:has-text("Sign in")')
    
    # Wait for navigation to complete
    page.wait_for_load_state("networkidle")
    
    return page


@pytest.fixture
def test_team_name() -> str:
    """Provide a test team name for team-related tests."""
    return os.environ.get("TEST_TEAM_NAME", "test-team")


@pytest.fixture
def test_token_name() -> str:
    """Provide a unique token name for API token tests."""
    import uuid
    return f"test-token-{uuid.uuid4().hex[:8]}"
