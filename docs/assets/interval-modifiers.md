# Interval Modifiers

Bruin supports adjusting the start and end of your data processing window using `interval_modifiers`. This is useful in situations where events might arrive late (e.g. offline mobile game events) or slightly ahead of schedule.

You can shift the start or end of the interval either forward or backward, depending on how you want to widen the processing window.

This works for SQL, Python, and ingestr assets and is primarily designed for regularly scheduled pipeline runs.

> [!IMPORTANT]
> Interval modifiers are **opt-in on the CLI** (`--apply-interval-modifiers`) and **applied automatically in Bruin Cloud**. See [Enabling interval modifiers](#enabling-interval-modifiers).

## How It Works

Use the `interval_modifiers` block in your asset definition to control how much to shift the window and supports both lookback and lookahead:

```yaml
interval_modifiers:
  start: -2h    # Shift start time back 2 hours
  end: 1h       # Shift end time forward 1 hour
```

## Conditional Interval Modifiers with Jinja

You can use [Jinja templating](./templating/templating.md) within the `interval_modifiers` values themselves to conditionally modify intervals based on dynamic conditions:

```yaml
interval_modifiers:
  start: '{% if start_timestamp|date_format("%H") == "00" %}-20d{% else %}0{% endif %}'
```

This example:

- Shifts the start time back by 20 days if the start timestamp is at midnight (hour = 00)
- Otherwise, applies no shift (0)

You can use any Jinja expressions, filters, and conditional logic to determine your interval modifiers dynamically. For more information about Jinja syntax and features, see the [Jinja templating documentation](./templating/templating.md).

## Pipeline-Level Defaults

You can set default interval modifiers at the pipeline level that will apply to all assets unless explicitly overridden. Define these in your pipeline's `default` section:

```yaml
default:
  interval_modifiers:
    start: 2h    # Default 2-hour shift for all assets
    end: 2h      # Default 2-hour shift for all assets
```

Individual assets can override these defaults by specifying their own `interval_modifiers`. If an asset doesn't specify interval modifiers, it will inherit the pipeline defaults.

### Examples

- `start: -2h` → shift start time back 2 hours
- `end: -1h` →  shift end time back 1 hour

Each interval is a scalar string with a single unit:

| Unit | Meaning      |
|------|--------------|
| ns   | Nanoseconds  |
| ms   | Milliseconds |
| s    | Seconds      |
| m    | Minutes      |
| h    | Hours        |
| d    | Days         |
| M    | Months       |

✅ Valid: `5m`, `2h`, `1d`, `-1M`, `500ms`, `1000ns`
❌ Invalid: `1h30m`, `2h:30m`, `90min`

## Enabling interval modifiers

Defining `interval_modifiers` in an asset or in the pipeline `default` block is not enough on its own — the modifiers still have to be *applied* at run time. Whether that happens automatically depends on where the pipeline runs.

| Where | Default | How to control it |
|-------|---------|-------------------|
| Bruin CLI (`bruin run`, `bruin render`) | **Off** | Pass `--apply-interval-modifiers` |
| Bruin Cloud (scheduled runs, manual runs, backfills) | **On** | Always applied; no flag or toggle needed |
| VS Code extension | Off | [Apply Interval Modifiers](/vscode-extension/configuration) setting |

### On the CLI

```bash
bruin run --apply-interval-modifiers my_asset
```

Without the flag, the run uses the interval as given and your `interval_modifiers` are ignored silently — no error, no warning. If you are debugging "my modifiers don't seem to do anything" locally, this is almost always the reason.

The reason it is opt-in on the CLI: you usually pass `--start-date` / `--end-date` yourself, and silently shifting the window you explicitly asked for would be surprising — for example when re-running a single day to reproduce an issue, or when comparing output against an exact window.

To confirm what a run will actually see, render the asset with and without the flag:

```bash
bruin render my_asset                              # unmodified interval
bruin render --apply-interval-modifiers my_asset   # modified interval
```

> [!NOTE]
> `--apply-interval-modifiers` is ignored when `--full-refresh` is used, since a full refresh does not process an interval. Bruin prints a warning when both are given.

### In Bruin Cloud

Bruin Cloud owns the schedule, so it computes the interval for each run itself and applies your `interval_modifiers` on top of it automatically. Scheduled runs, manual runs, and [backfill](/cloud/backfills) child runs all use the modified window — there is no flag to set.

This means the same pipeline can process a different window locally and in Cloud unless you add `--apply-interval-modifiers` to your local command. When reproducing a Cloud run on your machine, include the flag.

## Example

```yaml
name: example.products
type: bq.sql

interval_modifiers:
  start: -2h    # Shift start time back 2 hours
  end: 1h       # Shift end time forward 1 hour

columns:
  - name: product_id
    type: INTEGER
    primary_key: true
  - name: created_at
    type: TIMESTAMP
```

Let's say your pipeline is scheduled to run daily. Here's how the interval modifiers affect your processing window:

**Original interval:**

- Start: 2025-01-10 00:00:00
- End: 2025-01-11 00:00:00

**Modified interval:**

- Start: 2025-01-09 22:00:00 (shifted back 2 hours)
- End: 2025-01-11 01:00:00 (shifted forward 1 hour)

This ensures you capture any late-arriving data from the previous day and any early events from the next interval.
