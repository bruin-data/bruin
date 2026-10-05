# Upgrade Command

The `bruin upgrade` command updates your Bruin CLI installation to the latest version directly from the command line.

## Usage

```shell
bruin upgrade [options] [version]
```

`bruin update` is an alias for `bruin upgrade`.

### Arguments

**version** (optional): the release to install, such as `v0.20.0`. A missing `v` prefix is added for you. Defaults to the latest release.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--timeout` | `30s` | How long to wait when fetching the latest version and downloading the release. |

## Description

The upgrade command fetches the latest release from GitHub, downloads the appropriate binary for your operating system and architecture, and replaces the current `bruin` binary in place. If you already have the target version, it does nothing.

1. Fetches the latest version of Bruin from GitHub releases
2. Downloads the appropriate binary for your operating system and architecture  
3. Replaces the existing `bruin` binary

This eliminates the need to manually download and replace the CLI binary.

## Examples

### Upgrade to the Latest Version

Upgrade to the latest version:

```shell
bruin upgrade
```

### Upgrade to a Specific Version

```shell
bruin upgrade v0.20.0
```

## Prerequisites

The upgrade command downloads the release archive directly from GitHub, so it requires outbound network access.

## Platform Notes

### Windows

Windows does not allow replacing a running executable, so the new binary is written next to the current one and swapped in by a background script after `bruin upgrade` exits. The command prints `Upgrade prepared` instead of `Upgrade complete`.

## Error Handling

The command will fail gracefully if:

- The download fails due to network issues
- You don't have write permissions to the installation directory

## Security

The upgrade command downloads release artifacts from GitHub over HTTPS.

## See Also

- [`bruin version`](/commands/version) - Check your current Bruin version
- [Installation Guide](../getting-started/introduction/installation.md) - Initial installation instructions
