# Asset Health

Asset Health shows the recorded outcomes for an asset across its scheduled data intervals. Use it to find failures and gaps, inspect the runs behind an interval, and record a manual outcome when data was prepared outside a scheduled run.

## Open the health timeline

Open **Catalog → Assets**, select an asset, and find **Health** on its **Overview** page. Use the date range picker to choose the period you want to inspect.

Each bar represents a scheduled data interval. Larger date ranges can group several intervals into one bar. These are the time ranges the runs process, not the times when execution started or finished.

- Hover over a bar to see its interval, status, and run count.
- Click a bar to see the runs and manual entries that overlap it.
- Inspect the individual runs to understand a failure or a mixed result.

The timeline requires a scheduled pipeline. Continuous pipelines and pipelines without a schedule are not supported by this view.

## Understand the statuses

| Status | Meaning |
| --- | --- |
| Success | The recorded outcome is successful and successful runs cover the interval. |
| Failed | The recorded outcome is a failure, including a failure caused by an upstream dependency. |
| Checks failed | The asset's main task succeeded, but a blocking quality check failed. |
| Mixed | The bar combines different outcomes. Click it to inspect the runs or intervals involved. |
| Partially covered | Successful runs cover only part of the interval. |
| No runs | No recorded runs or manual entries overlap the interval. |
| Incomplete | The current scheduled interval has not ended yet. This does not by itself mean a task is running. |

A bar summarizes the interval; it does not replace run history. For overlapping runs, a newer result covering the whole interval can determine the displayed status, while a newer result covering only part of it can make the status mixed. Earlier runs remain available in the drill-down.

## Inspect an external dependency

From a pipeline's run details, click the asset waiting on an external dependency. Its asset instance page shows an **External Dependencies** section with a health bar beside each upstream asset.

1. Click a slot in the upstream health bar to inspect the runs for that interval.
2. Click the upstream asset's name to expand its pipeline information and coverage summary.
3. For partially covered slots, inspect the listed uncovered ranges.

See [cross-pipeline dependencies](/cloud/cross-pipeline) for configuring an external upstream.

## Add a manual entry

A manual entry records **Success** or **Failure** for an asset over a time range without executing it. For example, after preparing and checking data locally, you can record that outcome so a downstream dependency can recognize it.

1. On the asset's **Health** timeline, select the interval and click **+ manual entry**. Alternatively, expand an upstream in **External Dependencies** and click **Add manual entry**.
2. Review **Start Date** and **End Date**, both in **UTC**. Selecting a slot prefills its dates; you can adjust them. The displayed end date is inclusive.
3. Choose **Success** or **Failure**.
4. Add a note explaining what was done and why the entry is needed.
5. Click **Save manual entry**.

Creating, editing, or deleting manual entries requires permission to mark asset instances (`pipeline:run:asset-instance:mark-as`).

### Effect on downstream dependencies

A successful manual entry participates in dependency checks and can supply missing successful coverage. Choose the interval to match the data you have actually verified.

The original failed run keeps its status and logs. The manual entry is a separate record, visible with a **manual** label and its note. It applies to the upstream asset and can affect other downstream runs that depend on that asset, not just the run you opened.

This still declares an outcome for the interval: it is not an instruction to ignore the dependency. It does not execute the asset or repair its data.

### Edit or delete an entry

Click the relevant health bar to display its entries. Open the manual entry's action menu and choose **Edit** to change its dates, status, or note, or **Delete** to remove it after confirmation.

Deleting a manual entry removes that record from health and subsequent dependency checks. It leaves the original run history intact and does not undo downstream work that already ran.

## Related

- [Assets](/cloud/assets) — browse assets and their definitions.
- [Runs](/cloud/runs) — inspect execution history, logs, and run actions.
- [Cross-pipeline dependencies](/cloud/cross-pipeline) — configure dependencies between pipelines.
