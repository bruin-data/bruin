# E2E Tests for User & Team Management

This directory contains Playwright-based end-to-end tests for the User & Team Management features.

## Pages Tested

- `/user/api-tokens` - API Tokens management
- `/teams/{team}` - Team Settings
- `/user/profile` - Profile Settings

## Test Scenarios

| Test | Description |
|------|-------------|
| `test_api_tokens_page_loads` | Verify API tokens page loads with token list and create button |
| `test_create_api_token` | Create a new API token and verify it's displayed |
| `test_delete_api_token` | Delete an API token and verify removal |
| `test_team_settings_loads` | Verify team settings page loads with members list and tabs |
| `test_profile_settings_loads` | Verify profile page loads with all sections (profile, password, 2FA) |
| `test_update_profile_information` | Update profile info and verify success message |

## Required UI Element IDs

The following IDs should be present in the frontend:

- `#api-tokens-table` - Table/list of API tokens
- `#create-token-button` - Button to create a new token
- `#token-display-once` - Element displaying the newly created token (shown once)
- `#team-settings-tabs` - Tabs for team settings
- `#team-members-list` - List of team members
- `#profile-form` - Profile settings form

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
playwright install chromium
```

2. Configure environment variables:

```bash
export BASE_URL="http://localhost:3000"  # Application URL
export TEST_USER_EMAIL="test@example.com"  # Test user email
export TEST_USER_PASSWORD="testpassword"  # Test user password
export TEST_TEAM_NAME="test-team"  # Team name for team tests
export HEADLESS="true"  # Set to "false" to see browser
```

## Running Tests

Run all tests:

```bash
pytest test_user_team_management.py -v
```

Run specific test class:

```bash
pytest test_user_team_management.py::TestAPITokens -v
pytest test_user_team_management.py::TestTeamSettings -v
pytest test_user_team_management.py::TestProfileSettings -v
```

Run a specific test:

```bash
pytest test_user_team_management.py::TestAPITokens::test_api_tokens_page_loads -v
```

## Test Output

Tests will produce detailed output including:
- Pass/fail status for each test
- Screenshots on failure (if configured)
- Trace files for debugging (if enabled)
