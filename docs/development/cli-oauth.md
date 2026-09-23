# CLI personal-token OAuth contract

This document defines the Cloud backend required by `bruin login oauth`. These endpoints are a new integration contract, not the existing MCP OAuth implementation. The CLI changes alone do not provide the browser consent page or backend token issuance.

## Endpoints

The CLI uses the origin of `BRUIN_CLOUD_BASE_URL` (default `https://cloud.getbruin.com/api/v1`). HTTPS is required except on loopback development hosts. Register public client `bruin-cli`; no client secret is used.

### `GET /cli/oauth/authorize`

Accept `client_id=bruin-cli`, `response_type=code`, `redirect_uri`, `state`, `code_challenge`, `code_challenge_method=S256`, and `storage=repo|global`.

The redirect URI must be exactly `http://127.0.0.1:<ephemeral-port>/callback`. Validate the host, path and port structurally; do not allow arbitrary redirects. Bind the complete redirect URI, client, user, approved permissions and teams to a short-lived, hashed authorization code. The storage value is display context, not an authorization boundary, and never contains a local path or Git remote.

Use the existing Cloud session and personal-token permission checks. The page lets the user choose:

- One or more teams they belong to, with a default team when multiple are selected.
- Read (initial selection), Read-write, or Custom abilities.
- Expiration, defaulting to 90 days and using existing PAT expiration policies.

Read must use an explicit allowlist and exclude credential disclosure, execution and indirect write capabilities. Read-write includes the currently available read/write/delete and team-setting abilities within the user's role. Store an explicit snapshot of approved abilities rather than `*`. Custom uses existing PAT permission controls. Enforce team policies, including required MFA, and the user's live role on every API request.

After approval redirect with `code` and the original `state`. After rejection redirect with `error=access_denied` and `state`. Never put a PAT in a redirect or render it on the consent page.

### `POST /cli/oauth/token`

Accept form-encoded `grant_type=authorization_code`, `client_id`, `redirect_uri`, `code` and `code_verifier`. Validate PKCE S256 and all code bindings, and consume the code atomically once. Create a PAT only on successful exchange. Apply the existing PAT limits, auditing and rate limits.

Return HTTP 200 JSON with `Cache-Control: no-store`:

```json
{
  "access_token": "<new-personal-token>",
  "token_type": "Bearer",
  "token_id": "123",
  "account": "person@example.com",
  "default_team": "acme",
  "expires_at": "2026-12-20T12:00:00Z",
  "abilities": ["pipeline:list"],
  "teams": ["acme"]
}
```

`token_id` is a string. `expires_at` is an RFC3339 timestamp, or `null` for an explicitly allowed non-expiring PAT. Return no refresh token. The CLI refuses redirects at the exchange endpoint and does not log response bodies or authorization material. Replaying or retrying a consumed code must fail without creating another PAT.

### `POST /cli/oauth/revoke`

Authenticate the PAT with `Authorization: Bearer <token>` and revoke only that token. This operation must be available to its owner regardless of Read/Read-write/Custom abilities. Return HTTP 204 or 200. Use this for explicit `logout --revoke` and cleanup after a failed local save. Treat already-revoked credentials idempotently where the existing PAT infrastructure permits it.

## Deployment and verification

Deploy backend and consent UI before distributing the CLI feature. Verify success, denial, expired and reused codes, incorrect PKCE/state/redirect, multiple teams, role reductions, read-only authorization and failed local persistence in a development Cloud environment. The CLI unit tests exercise the exchange using a local HTTP server; they do not verify a deployed Cloud implementation.
