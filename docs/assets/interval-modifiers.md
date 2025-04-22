# Interval Modifiers: Adjusting Your Processing Window

Bruin supports adjusting the start and end of your data processing window using `interval_modifiers`. This is useful in situations where events might arrive late (e.g. offline mobile game events) or slightly ahead of schedule.

You can shift the start or end of the interval either forward or backward, depending on how you want to widen the processing window.

This works for both SQL and Python assets and is primarily designed for regularly scheduled pipeline runs.

## How It Works

Use the `interval_modifiers` block in your asset definition to control how much to shift the window and supports both lookback and lookahead:

```yaml
interval_modifiers:
  start: -2h    # Shift start time back 2 hours
  end: 1h       # Shift end time forward 1 hour
```

### Examples:
- `start: -2h` → shift start time back 2 hours
- `end: -1h` →  shift end time back 1 hour

Each interval is a scalar string with a single unit:

| Unit | Meaning |
|------|---------|
| s    | Seconds |
| m    | Minutes |
| h    | Hours   |
| d    | Days    |
| M    | Months  |

✅ Valid: `5m`, `2h`, `1d`,`-1M`
❌ Invalid: `1h30m`, `2h:30m`, `90min`

## Usage

To apply interval modifiers when running your pipeline, you must include the `--apply-interval-modifiers` flag in your run command:

```bash
bruin run --apply-interval-modifiers my_asset 
```

Without this flag, the pipeline will use the default interval without any modifications.

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

