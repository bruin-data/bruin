# `login` Command

`bruin login oauth` opens Bruin Cloud in your browser to create a personal access token. Inside a Bruin project, choose whether to save the login for the current Git repository or globally.

Bruin checks for `.bruin.yml` at the Git root first. If it is missing, Bruin searches the repository for `pipeline.yml` or `pipeline.yaml`, using the same directory exclusions as other pipeline commands. If neither is found, or you are outside a Git repository, login uses global storage automatically. Explicit `--repo` requires one of these files in a Git repository.

Use ↑/↓ to navigate the terminal menus and Enter to select. Press Ctrl+C to cancel.

> This CLI flow requires the Cloud CLI OAuth endpoints described in the [backend contract](../development/cli-oauth.md). The existing MCP OAuth endpoints cannot issue the tokens this command needs.

```bash
bruin login oauth
bruin login oauth --repo
bruin login oauth --global
```

The browser approval page selects the teams the personal token can access, its expiration, and Read, Read-write, or Custom permissions. Read-write includes deletion and team settings where your own role allows them. The token always acts as you. Choosing repository storage does not restrict Cloud access to that repository.

## Existing connections

If the selected target already has a login, Bruin asks whether to use it, sign in again, or cancel before opening the browser. When several `bruin` connections exist, choose which connection to update. You can also specify it directly:

```bash
bruin login oauth --repo --environment production --connection cloud
```

Keeping an existing connection leaves the configuration unchanged. Signing in again replaces its token only after authorization succeeds. The login flags choose where to save the token; they do not persist a connection selection for Cloud commands.

Without an interactive terminal, specify `--repo` or `--global`. Use `--connection` and `--environment` to disambiguate existing connections, and `--reauth` to explicitly authorize replacing an existing login. Browser consent is still required.

## Storage

Repository login creates or updates a connection of type `bruin` in the Git root's `.bruin.yml`. If a pipeline exists but `.bruin.yml` is missing, the configuration file is created after successful authorization:

```yaml
default_environment: default
environments:
  default:
    connections:
      bruin:
        - name: cloud
          api_token: "<personal-token>"
          api_url: https://cloud.getbruin.com/api/v1
cloud:
  default_team: acme
```

The `cloud` section only stores `default_team`. You can change it later with `bruin cloud config set-team <company_prefix>`.

The file is ignored by Git and written with owner-only permissions on POSIX systems. Bruin refuses to write a token if this file is already tracked by Git. Repository tokens remain plaintext and can be read by processes running as your user. Existing unrelated configuration and environment-variable references are preserved.

Global login saves the same YAML structure, including the token in `api_token`, to `$XDG_CONFIG_HOME/bruin/cloud.yml`, or `~/.config/bruin/cloud.yml` when that variable is unset. On POSIX systems, the file is written with `0600` permissions and its directory is created with `0700` permissions. Global tokens are plaintext, just like repository tokens.

## Credential precedence

Cloud commands choose a token in this order:

1. `--api-key`
2. `BRUIN_CLOUD_API_KEY`
3. The repository `bruin` connection
4. Global login

A single existing `bruin` connection continues to work without running login. When multiple `bruin` connections exist, supply the desired token using `--api-key` or `BRUIN_CLOUD_API_KEY`, or keep a single repository `bruin` connection. Invalid configuration, an empty repository token, or an expired or unauthorized token causes an error; Bruin does not silently try a lower-priority token.

`--team` or `BRUIN_CLOUD_TEAM` overrides the repository's `cloud.default_team`. If neither supplies a team and the active token is global, the global login's default team is used.

New logins bind credentials to the Cloud API destination. Changing `BRUIN_CLOUD_BASE_URL` cannot redirect a stored login to a different destination. Explicit `--api-key` and environment tokens retain their existing behavior.

## Inspect and remove a login

```bash
bruin auth status
bruin logout --repo
bruin logout --global
bruin logout --repo --revoke
```

Status shows the locally selected credential source and team without displaying the token. It does not check API validity. Repository logout requires a single `bruin` connection and removes it; global logout removes its `bruin` connection from the global file. Add `--revoke` to invalidate the personal token in Cloud too. Without it, other copies of that token remain usable.

Removing a repository login can expose a lower-priority global login. `--api-key` and environment variables are unaffected by logout.

## Troubleshooting

- **Browser unavailable:** use `--no-browser` and open the printed URL manually. The browser must be able to reach the CLI's loopback callback; SSH or containers may require port forwarding.
- **Existing login in a non-interactive terminal:** rerun interactively to keep it, or pass `--reauth` to replace it.
- **Token expired:** run login again. This flow does not automatically refresh personal tokens.
- **Insufficient permission:** change the token's permissions in Cloud or create another token with the required abilities. Your current user role still applies.
- **Configuration changed during login:** the old file is preserved; retry after reviewing the changes.
- **Cannot save a new token:** Bruin attempts to revoke the unsaved token. If revocation fails, remove the reported token ID from your personal access tokens page in Cloud.
