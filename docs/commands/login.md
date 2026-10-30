# `cloud login` Command

Sign in to Bruin Cloud:

```bash
bruin cloud login
```

1. Choose **This repository** to save the login in your Git repository, or **Global** to use it across projects. If `.bruin.yml` is missing, the repository option offers to create it. Outside a Git repository, use `bruin cloud login --global`.
2. Sign in to your Cloud account in the browser.
3. Select your teams, [access level](/cloud/api-tokens#permissions), and access duration. If you select multiple teams, choose a default team.
4. Click **Authorize**, then return to the terminal.

Use ↑/↓ and Enter for terminal selections. Press Ctrl+C to cancel.

Try a Cloud command after signing in:

```bash
bruin cloud projects list
```

## Choose where to sign in

From any directory in your Git repository, save the login for that repository:

```bash
bruin cloud login --repo
```

Repository login works in any Git repository, including worktrees. Existing `.bruin.yml` or pipeline files are not required. When `.bruin.yml` is missing, `bruin cloud login` offers **This repository (create .bruin.yml)** and **Global**. Choosing repository login creates the file at the repository or worktree root after successful authorization and adds it to `.gitignore`. All pipelines in that repository share the login.

`--repo` selects repository login directly and also creates `.bruin.yml` if needed. Outside a Git repository, Bruin asks you to run from one or explicitly use `--global`; it does not automatically switch to global login.

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
bruin cloud auth status
```

If the result is unexpected, check for `--api-key` or `BRUIN_CLOUD_API_KEY`, which override saved logins. A repository login takes precedence over a global login.

To choose a team for a command:

```bash
bruin cloud teams list
bruin cloud projects list --team acme
```

To save a default team for your current repository, run `bruin cloud config set-team acme`.

## Sign out

Choose which login to sign out of:

```bash
bruin cloud logout
```

Select **This repository** or **Global** with ↑/↓ and Enter. Outside a Git repository, only **Global** is offered. Press Esc or Ctrl+C to cancel without removing either login.

To skip the menu, remove the login for your current repository:

```bash
bruin cloud logout --repo
```

Remove your global login:

```bash
bruin cloud logout --global
```

By default, logout only removes the local login; it does not revoke the token in Cloud. Add `--revoke` to also invalidate the selected token:

```bash
bruin cloud logout --revoke
```

This still opens the repository/global selection menu. Combine `--revoke` with `--repo` or `--global` to skip the menu. Bruin revokes the token before removing the local login and reports both steps. Other copies of the same token also stop working. If revocation fails, the local login is kept so you can retry.

Without an interactive terminal, specify `--repo` or `--global`.

## Options

| Flag | Use |
|------|-----|
| `--repo` | Save the login in the current Git repository, creating `.bruin.yml` if needed. |
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
- **No Git repository found:** run from a Git repository, or use `--global`. Existing Bruin project files are not required for repository login.
- **Global credential store unavailable:** unlock your operating system's credential store and retry, or use `--repo` from your project.
