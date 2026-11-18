# Git account and attribution policy

Follow this policy when preparing commits, pushes, pull request titles and descriptions, or Git-related comments. It does not grant permission to commit, push, or rewrite history.

## Keep the contributor's approved identity

Use the contributor's approved, repository-specific Git identity. Before committing or pushing, inspect the effective author and committer:

```sh
git var GIT_AUTHOR_IDENT
git var GIT_COMMITTER_IDENT
```

Some local setups also record the approved identity in these optional configuration values:

```sh
git config --get gitAccountPolicy.name
git config --get gitAccountPolicy.email
```

If these values are configured, check that the author and committer match them. If they are absent or the intended identity is unclear, ask the contributor rather than choosing an identity or changing Git configuration.

Do not substitute a bot identity, forge another person's authorship, or change credentials, remotes, or signing keys to make a contribution. Local Git identity does not establish ownership of a GitHub account.

Preserve legitimate human authorship and co-author credits when incorporating third-party work. If a local identity check conflicts with preserving that authorship, ask for an exception rather than bypassing the check.

## Use factual commit and pull request text

Write natural, factual subjects and descriptions explaining the change. Do not add AI or bot credits, signatures, advertising, generated-by footers, agent disclaimers, or bot co-author trailers. Apply the same rule to pull request titles, descriptions, and Git-related comments. Remove prohibited attribution from drafts before submitting them.

Mentioning a product as part of an actual technical change is fine. Do not remove legitimate human credits, provenance, or license notices from source files.

## Respect hooks and existing history

Run the repository's existing hooks and respect their failures. Do not bypass them with `--no-verify`, a `core.hooksPath` override, or environment changes intended to disable checks.

Do not rewrite existing history, amend someone else's commits, or force-push to remove attribution without explicit approval of the exact history scope. Prefer correcting new drafts before they are committed or published.

## Enforcement limits

This document provides contribution guidance. It does not install Git hooks, add CI checks, verify hosting-account ownership, or enforce a server-side policy.

Any separately installed local hooks remain local checks. Someone controlling Git configuration can bypass them, and they cannot govern web-created commits, remote bots, pull request forms, or hosting credentials. Do not describe local checks as server-side enforcement or account verification.
