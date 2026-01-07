"""
User & Team Management E2E Tests.

This module contains Playwright-based tests for:
- API Tokens page (/user/api-tokens)
- Team Settings (/teams/{team})
- Profile Settings (/user/profile)

Test IDs expected in the UI:
- #api-tokens-table
- #create-token-button
- #token-display-once
- #team-settings-tabs
- #team-members-list
- #profile-form
"""

import pytest
from playwright.sync_api import Page, expect


class TestAPITokens:
    """Tests for the API Tokens page (/user/api-tokens)."""

    def test_api_tokens_page_loads(
        self, authenticated_page: Page, base_url: str
    ) -> None:
        """Test that the API tokens page loads correctly.
        
        Verifies:
        - Token list/table appears (#api-tokens-table)
        - Create token button exists (#create-token-button)
        """
        page = authenticated_page
        
        # Navigate to API tokens page
        page.goto(f"{base_url}/user/api-tokens")
        page.wait_for_load_state("networkidle")
        
        # Verify token list/table appears
        tokens_table = page.locator("#api-tokens-table")
        expect(tokens_table).to_be_visible()
        
        # Verify create token button exists
        create_button = page.locator("#create-token-button")
        expect(create_button).to_be_visible()

    def test_create_api_token(
        self, authenticated_page: Page, base_url: str, test_token_name: str
    ) -> None:
        """Test creating a new API token.
        
        Verifies:
        - Click create token button
        - Fill token name and permissions
        - Token is created and displayed once (#token-display-once)
        """
        page = authenticated_page
        
        # Navigate to API tokens page
        page.goto(f"{base_url}/user/api-tokens")
        page.wait_for_load_state("networkidle")
        
        # Click create token button
        create_button = page.locator("#create-token-button")
        create_button.click()
        
        # Wait for the create token form/modal to appear
        page.wait_for_selector('input[name="token-name"], #token-name-input')
        
        # Fill in token name
        page.fill('input[name="token-name"], #token-name-input', test_token_name)
        
        # Select permissions if available (check a read permission checkbox)
        read_permission = page.locator('input[type="checkbox"][name*="read"], #permission-read')
        if read_permission.count() > 0:
            read_permission.first.check()
        
        # Submit the form to create the token
        page.click('button[type="submit"], #create-token-submit, button:has-text("Create")')
        
        # Wait for token creation to complete
        page.wait_for_load_state("networkidle")
        
        # Verify the token is displayed once (for copying)
        token_display = page.locator("#token-display-once")
        expect(token_display).to_be_visible()
        
        # Verify the token value is not empty
        token_value = token_display.text_content()
        assert token_value and len(token_value.strip()) > 0, "Token should be displayed"

    def test_delete_api_token(
        self, authenticated_page: Page, base_url: str
    ) -> None:
        """Test deleting an API token.
        
        Verifies:
        - Click delete on a token
        - Confirm deletion
        - Token is removed from list
        """
        page = authenticated_page
        
        # Navigate to API tokens page
        page.goto(f"{base_url}/user/api-tokens")
        page.wait_for_load_state("networkidle")
        
        # Get the tokens table
        tokens_table = page.locator("#api-tokens-table")
        expect(tokens_table).to_be_visible()
        
        # Find delete buttons for tokens
        delete_buttons = page.locator(
            '#api-tokens-table button[data-action="delete"], '
            '#api-tokens-table .delete-token-button, '
            '#api-tokens-table button:has-text("Delete")'
        )
        
        # Check if there are any tokens to delete
        if delete_buttons.count() == 0:
            pytest.skip("No tokens available to delete")
        
        # Get the initial count of tokens
        initial_token_count = delete_buttons.count()
        
        # Get the first token's identifier for verification
        first_token_row = page.locator("#api-tokens-table tbody tr, #api-tokens-table [data-token-row]").first
        token_identifier = first_token_row.get_attribute("data-token-id") or first_token_row.text_content()
        
        # Click delete on the first token
        delete_buttons.first.click()
        
        # Handle confirmation dialog/modal
        confirm_button = page.locator(
            'button:has-text("Confirm"), '
            'button:has-text("Yes"), '
            '#confirm-delete-button, '
            '[data-action="confirm-delete"]'
        )
        
        # Wait for and click confirm button if present
        if confirm_button.count() > 0:
            confirm_button.first.click()
        
        # Wait for the deletion to complete
        page.wait_for_load_state("networkidle")
        
        # Verify the token count has decreased
        delete_buttons_after = page.locator(
            '#api-tokens-table button[data-action="delete"], '
            '#api-tokens-table .delete-token-button, '
            '#api-tokens-table button:has-text("Delete")'
        )
        
        # Token should be removed from the list
        assert delete_buttons_after.count() < initial_token_count, \
            "Token count should decrease after deletion"


class TestTeamSettings:
    """Tests for the Team Settings page (/teams/{team})."""

    def test_team_settings_loads(
        self, authenticated_page: Page, base_url: str, test_team_name: str
    ) -> None:
        """Test that the team settings page loads correctly.
        
        Verifies:
        - Team members list appears (#team-members-list)
        - Settings tabs appear (#team-settings-tabs)
        """
        page = authenticated_page
        
        # Navigate to team settings page
        page.goto(f"{base_url}/teams/{test_team_name}")
        page.wait_for_load_state("networkidle")
        
        # Verify team members list appears
        members_list = page.locator("#team-members-list")
        expect(members_list).to_be_visible()
        
        # Verify settings tabs appear
        settings_tabs = page.locator("#team-settings-tabs")
        expect(settings_tabs).to_be_visible()


class TestProfileSettings:
    """Tests for the Profile Settings page (/user/profile)."""

    def test_profile_settings_loads(
        self, authenticated_page: Page, base_url: str
    ) -> None:
        """Test that the profile settings page loads correctly.
        
        Verifies:
        - Profile form displays (#profile-form)
        - Profile section exists
        - Password section exists
        - 2FA section exists
        """
        page = authenticated_page
        
        # Navigate to profile settings page
        page.goto(f"{base_url}/user/profile")
        page.wait_for_load_state("networkidle")
        
        # Verify profile form displays
        profile_form = page.locator("#profile-form")
        expect(profile_form).to_be_visible()
        
        # Verify profile section exists (name/email fields)
        profile_section = page.locator(
            '#profile-section, '
            '[data-section="profile"], '
            'section:has-text("Profile")'
        )
        expect(profile_section.first).to_be_visible()
        
        # Verify password section exists
        password_section = page.locator(
            '#password-section, '
            '[data-section="password"], '
            'section:has-text("Password")'
        )
        expect(password_section.first).to_be_visible()
        
        # Verify 2FA section exists
        twofa_section = page.locator(
            '#twofa-section, '
            '#2fa-section, '
            '[data-section="2fa"], '
            'section:has-text("Two-Factor"), '
            'section:has-text("2FA")'
        )
        expect(twofa_section.first).to_be_visible()

    def test_update_profile_information(
        self, authenticated_page: Page, base_url: str
    ) -> None:
        """Test updating profile information.
        
        Verifies:
        - Update name/email fields
        - Save changes
        - Success message appears
        """
        page = authenticated_page
        
        # Navigate to profile settings page
        page.goto(f"{base_url}/user/profile")
        page.wait_for_load_state("networkidle")
        
        # Verify profile form is visible
        profile_form = page.locator("#profile-form")
        expect(profile_form).to_be_visible()
        
        # Get the name input field
        name_input = page.locator(
            '#profile-form input[name="name"], '
            '#profile-form input[name="full_name"], '
            '#profile-form input[name="displayName"], '
            '#profile-form #name-input'
        )
        
        # Update the name field with a test value
        if name_input.count() > 0:
            # Get current value to restore later or use for verification
            current_name = name_input.first.input_value()
            test_name = f"{current_name} Updated" if current_name else "Test User Updated"
            
            # Clear and fill with new value
            name_input.first.fill(test_name)
        
        # Find and click the save/submit button
        save_button = page.locator(
            '#profile-form button[type="submit"], '
            '#save-profile-button, '
            'button:has-text("Save"), '
            'button:has-text("Update Profile")'
        )
        expect(save_button.first).to_be_visible()
        save_button.first.click()
        
        # Wait for the save operation to complete
        page.wait_for_load_state("networkidle")
        
        # Verify success message appears
        success_message = page.locator(
            '.success-message, '
            '[data-testid="success-message"], '
            '.alert-success, '
            '.toast-success, '
            '[role="alert"]:has-text("success"), '
            ':has-text("Profile updated"), '
            ':has-text("Changes saved")'
        )
        expect(success_message.first).to_be_visible()
