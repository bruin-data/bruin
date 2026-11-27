# Dac Command

The `dac` command runs [dac](https://getbruin.com/docs/dac/), Bruin's dashboards-as-code tool. You don't need to install dac yourself: the first time you run `bruin dac`, Bruin downloads the dac version it is pinned to, checks the download against a SHA-256 built into Bruin, and keeps it in Bruin's home directory. Later runs reuse that copy and need no network access.

```bash
bruin dac init my-dashboards
bruin dac validate --dir my-dashboards
bruin dac check --dir my-dashboards
bruin dac serve --dir my-dashboards --open
```

Every argument after `dac` goes to dac unchanged, including `--help`. Run `bruin dac --help` for dac's commands and flags, and see the [dac documentation](https://getbruin.com/docs/dac/) for how to write dashboards.

## How it works

- dac runs as a separate process. Its stdin, stdout and stderr are your terminal's, and `bruin dac` exits with dac's exit code.
- Ctrl-C stops dac, so stopping `bruin dac serve` stops the server.
- dac runs every query through the same `bruin` binary you started it with, not another `bruin` on your `PATH`.
- `bruin dac` always runs the pinned dac. It ignores any `dac` already on your `PATH`, and dac's `upgrade` and `version` commands are hidden. To get a newer dac, upgrade Bruin.
- If you have opted out of Bruin's telemetry with `TELEMETRY_OPTOUT`, dac's telemetry is turned off too.

## Where dac is installed

The pinned release goes into `~/.bruin/dac/<version>/dac` (`dac.exe` on Windows), or under `$BRUIN_HOME/dac/` when `BRUIN_HOME` is set. Bruin's Docker image ships with it preinstalled.

Release archives exist for macOS (Apple Silicon and Intel), Linux (x86_64 and arm64) and Windows (x86_64). On other platforms `bruin dac` exits with an error.

## When the download fails

If the download fails, its checksum doesn't match, or you are offline on the first run, `bruin dac` prints what went wrong and how to install dac by hand. Nothing is left half-installed, so the next run starts the download again. To install manually:

1. Download the archive for your platform from the [dac release](https://github.com/bruin-data/dac/releases) named in the error.
2. Check its SHA-256 against the one printed in the error.
3. Extract `dac` (or `dac.exe`) to the path printed in the error, and on macOS and Linux make it executable with `chmod +x`.

## Using your own dac build

Set `BRUIN_DAC_BINARY` to run a different dac binary, for example a local build when you're working on dac itself:

```bash
BRUIN_DAC_BINARY=~/code/dac/bin/dac bruin dac serve --dir my-dashboards
```

Bruin skips the managed install when this variable is set.
