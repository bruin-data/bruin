# `version` Command

The `version` command prints the installed Bruin CLI version and the latest version available on GitHub.

## Usage

```bash
bruin version [flags]
```

`bruin --version` and `bruin -v` print the same plain-text output.

### Flags

| Flag | Alias | Default | Description |
|------|-------|---------|-------------|
| `--output` | `-o` | `plain` | Output format: `plain` or `json`. |
| `--timeout` | | `5s` | How long to wait when fetching the latest version from GitHub. |

## Examples

```bash
bruin version
```

```text
Current: v0.11.770 (3f9c2e7a1b4d8e6f0a2c5b9d7e1f4a6c8b0d2e3f)
Latest: v0.11.770
```

```bash
bruin version -o json
```

```json
{"version":"v0.11.770","commit":"3f9c2e7a1b4d8e6f0a2c5b9d7e1f4a6c8b0d2e3f","latest":"v0.11.770"}
```

If GitHub can't be reached before the timeout, `Latest` shows `<unknown: error fetching version information>`.

To install a newer version, run [`bruin upgrade`](/commands/upgrade).
