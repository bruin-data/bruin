# Upgrade Command

The `bruin upgrade` command updates your Bruin CLI installation to the latest version directly from the command line.

## Usage

```shell
bruin upgrade [flags]
```

## Description

The upgrade command fetches the latest release from GitHub, downloads the appropriate binary for your operating system and architecture, and replaces the current `bruin` binary in place.

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

## Prerequisites

The upgrade command downloads the release archive directly from GitHub, so it requires outbound network access.

## Platform Notes

### Windows
On Windows, make sure to run the upgrade command in:
- **Git Bash** (recommended)
- **WSL (Windows Subsystem for Linux)**

The command will warn you if you're on Windows to ensure you're using the correct terminal environment.

## Error Handling

The command will fail gracefully if:
- The download fails due to network issues
- You don't have write permissions to the installation directory

## Security

The upgrade command downloads release artifacts from GitHub over HTTPS.

## See Also

- [bruin version] - Check your current Bruin version
- [Installation Guide](../getting-started/introduction/installation.md) - Initial installation instructions
