---
name: record-vhs-demo
description: Create, update, render, and visually verify polished Bruin CLI terminal demos with VHS. Use when recording a terminal walkthrough, adding or editing a .tape file, exporting a demo video or GIF, improving terminal-demo styling or readability, or reproducing a Bruin command sequence for documentation or social media.
---

# Record VHS Demo

Create concise, deterministic terminal recordings whose source remains reusable in the repository.

## Repository layout

- Store every canonical `.tape` file under `<repo>/resources/`, using a descriptive kebab-case name.
- Store stable demo fixtures under `<repo>/resources/<demo-name>/` when the tape needs them.
- Store generated videos, GIFs, screenshots, and contact sheets under `<repo>/.context/vhs-demos/` unless the user explicitly requests a tracked documentation asset.
- Confirm `.context/` is ignored with `git check-ignore`. If it is not, add `.context/` to the repository's local `.git/info/exclude`; do not add generated demo artifacts to the tracked `.gitignore` merely for a recording.
- Never put real credentials, tokens, personal paths, or production data in a tape or fixture. Use unmistakably fake values.

## Workflow

1. Confirm the short story the recording should tell. Show only commands and output that support it.
2. Validate the underlying Bruin behavior outside VHS before recording it.
3. Build the appropriate binary:
   - Prefer `make build-no-duckdb` and `bin/bruin-no-duckdb` for demos that do not need DuckDB.
   - Use `make build` and `bin/bruin` when the demo needs DuckDB.
4. Create or edit the canonical tape under `resources/`. Put any reproducible fixtures beside it under a demo-specific directory.
5. Render from the tape's directory with the bundled script:

   ```bash
   python3 .agents/skills/record-vhs-demo/scripts/render_demo.py resources/<demo-name>.tape
   ```

6. Inspect the generated contact sheet with an image-viewing tool. Extract and inspect a full-resolution frame when text color, masking, alignment, or spacing needs closer review.
7. Iterate until the recording has no command failures, accidental secrets, unreadable ANSI colors, clipped lines, excessive dead time, or irrelevant output.

## Visual defaults

Start with these settings unless the user asks for a different format:

```text
Set Shell bash
Set FontSize 20
Set Width 1280
Set Height 720
Set Margin 0
Set Padding 28
Set LineHeight 1.15
Set TypingSpeed 30ms
Set Framerate 30
Set CursorBlink false
Set Theme "GitHub Dark"
```

- Use a plain dark terminal canvas without a window frame, title bar, or outer margin.
- Keep internal padding so text does not touch the video edge.
- Prefer Bash for predictable prompts and hidden setup.
- Use a leading newline in `PS1` to add space between prompt blocks without changing regular output line spacing:

  ```bash
  export PS1="$(tput setaf 6)\n❯$(tput sgr0) "
  ```

- In the hidden setup, run `unset NO_COLOR`, set `TERM=xterm-256color`, and set `CLICOLOR=1`. The agent environment may otherwise suppress Bruin's ANSI output.
- Do not add `--no-color` to a demo command.
- For a one-asset Bruin demo, make worker coloring deterministic by wrapping the binary with `--workers 1` during hidden setup. This avoids a random worker selecting an unreadable ANSI color while keeping the visible command clean.
- Hide setup commands, paths, fixture preparation, aliases/functions, and terminal clearing.
- Do not show `--help`, installation steps, decorative titles, or commentary that delays the feature demonstration unless requested.
- Use `--no-validation`, `--no-timestamp`, or `--minimal-logs` only when they remove irrelevant output without hiding the behavior being demonstrated.
- Add short sleeps after meaningful output, not after every command. Keep social clips compact.

## Reliable tape setup

- Run tapes from their own directory. Resolve repository files from `git rev-parse --show-toplevel`, not fragile multi-level relative aliases.
- Use `mktemp -d` for disposable working fixtures. Copy tracked examples into the temporary directory during hidden setup.
- Create the `Output` directory before rendering. The bundled script does this automatically.
- Keep the visible commands representative of what a user would actually type.

## Verification bar

Do not call a demo complete until all of the following are true:

- `vhs validate` passes.
- VHS exits successfully and produces a non-empty artifact.
- VHS reports no internal `error:` line; some versions can emit a partial video after an EOF error while still exiting with status 0.
- `ffprobe` reports a valid duration.
- The contact sheet has been visually inspected.
- A full-resolution output frame is inspected when color or text readability matters.
- Every command in the recording succeeds.
- Expected values are visible and any credentials are masked.
- Prompt spacing affects prompt blocks only, not ordinary output lines.
- ANSI colors have sufficient contrast on the selected dark theme.

The renderer reports the artifact and QA image paths; include the final artifact path in the handoff.
