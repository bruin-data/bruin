# `cloud login` Command

Sign in to Bruin Cloud:

```bash
bruin cloud login
```

1. Choose **This repository** to use the login for your current project, or **Global** to use it across projects. Outside a Bruin project, use Global.
2. Sign in to your Cloud account in the browser.
3. Select your teams, [access level](/cloud/api-tokens#permissions), and access duration. If you select multiple teams, choose a default team.
4. Click **Authorize**, then return to the terminal.

Use ↑/↓ and Enter for terminal selections. Press Ctrl+C to cancel.

Try a Cloud command after signing in:

```bash
bruin cloud projects list
```

## Choose where to sign in

From your Bruin project directory, save the login for that repository:

```bash
bruin cloud login --repo
```

To use the login across projects:

```bash
bruin cloud login --global
```

If you already have a login, select **Use existing login**, **Sign in again**, or **Cancel**.

To renew access or choose different permissions:

```bash
bruin cloud login --repo --reauth
```

Use `--global --reauth` to replace a global login.

## Check your login

See which login and default team you are using:

```bash
bruin auth status
```

If the result is unexpected, check for `--api-key` or `BRUIN_CLOUD_API_KEY`, which override saved logins. A repository login takes precedence over a global login.

To choose a team for a command:

```bash
bruin cloud teams list
bruin cloud projects list --team acme
```

To save a default team for your current repository, run `bruin cloud config set-team acme`.

## Sign out

Remove the login for your current repository:

```bash
bruin logout --repo
```

Remove your global login:

```bash
bruin logout --global
```

Add `--revoke` to also invalidate the token in Cloud, including any other copies:

```bash
bruin logout --global --revoke
```

## Options

| Flag | Use |
|------|-----|
| `--repo` | Sign in for the current Bruin project repository. |
| `--global` | Sign in across projects. |
| `--no-browser` | Open the printed authorization URL yourself. |
| `--reauth` | Replace an existing login. |
| `--connection <name>` | Choose the repository connection to create or update. |
| `--environment <name>` | Choose the repository environment to use. |

For a specific repository connection:

```bash
bruin cloud login --repo --environment production --connection cloud
```

Without an interactive terminal, specify `--repo` or `--global`; add `--reauth` when replacing an existing login. You still need to approve access in a browser. For CI, use an [API token](/cloud/api-tokens#using-a-token-with-the-cli).

## Troubleshooting

- **Browser did not open:** rerun with `--no-browser`, then open the printed URL on the same computer.
- **Access expired or permissions are insufficient:** sign in again with `--reauth` and choose the access you need.
- **No Bruin project found:** run from a Bruin project in a Git repository, or use `--global`.
- **Global credential store unavailable:** unlock your operating system's credential store and retry, or use `--repo` from your project.
