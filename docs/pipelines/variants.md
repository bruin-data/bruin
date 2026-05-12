# Pipeline Variants

Variants let a single `pipeline.yml` produce multiple concrete pipelines from one source file. Each variant overrides a subset of the pipeline's [custom variables](/variables/custom), and Bruin renders the templated identity fields (`name`, `schedule`, etc.) per variant.

A common use case: one logical pipeline that needs to run separately for several clients, regions, or environments without copy-pasting the YAML.

## Quick Example

```yaml
# pipeline.yml
name: "{{ var.client }}_pipe"
schedule: "{{ var.schedule }}"

variables:
  client:
    type: string
    default: client_a
  region:
    type: string
    default: us
  schedule:
    type: string
    default: "@daily"

variants:
  client_alpha:
    client: alpha
    region: us
    schedule: "@hourly"
  client_beta:
    client: beta
    region: eu
    schedule: "0 6 * * *"
  client_gamma:
    client: gamma
    region: ap
    schedule: "@weekly"
```

This single file declares **three** concrete pipelines:

| Variant | Rendered name | Schedule | Variables |
|---|---|---|---|
| `client_alpha` | `alpha_pipe` | `@hourly` | `client=alpha, region=us` |
| `client_beta` | `beta_pipe` | `0 6 * * *` | `client=beta, region=eu` |
| `client_gamma` | `gamma_pipe` | `@weekly` | `client=gamma, region=ap` |

## Defining Variants

```yaml
variants:
  <variant_name>:
    <variable_name>: <override_value>
    ...
```

Rules:

- **Variant name** must match `[a-zA-Z0-9_-]+`.
- **Variable names** under each variant must reference variables already declared in the pipeline's `variables:` block. Unknown names fail validation with `references unknown variable "X"`.
- A variant can override **any subset** of variables; unmentioned variables keep their `default` value.
- Variant overrides must match the type of the underlying variable (e.g., a variable typed as `integer` cannot be overridden with a string).
## Running a Variant

When a `pipeline.yml` declares variants, you must pick one with `--variant`:

```bash
bruin run --variant client_alpha
```



## Listing Variants

```bash
bruin internal list-variants <path-to-pipeline>
```
## Asset Body Example


```sql
/* @bruin
name: "{{ var.client }}_users_{{ var.region }}"
type: bq.sql
materialization:
  type: table
@bruin */

SELECT
  '{{ var.client }}' AS client,
  '{{ var.region }}' AS region,
  user_id,
  email,
  signed_up_at
FROM `analytics_{{ var.region }}.raw_users`
WHERE region_code = '{{ var.region }}'
  AND tenant = '{{ var.client }}';
```

How each variant materializes:

| Variant (`client`, `region`) | Asset name | Reads from | Writes to |
|---|---|---|---|
| `client_alpha` (alpha, us) | `alpha_users_us` | `analytics_us.raw_users` | `alpha_users_us` |
| `client_beta` (beta, eu) | `beta_users_eu` | `analytics_eu.raw_users` | `beta_users_eu` |
| `client_gamma` (gamma, ap) | `gamma_users_ap` | `analytics_ap.raw_users` | `gamma_users_ap` |

## Complete Pipeline Example

A full, runnable variant pipeline template lives in the repo at [`templates/variant-example`](https://github.com/bruin-data/bruin/tree/main/templates/variant-example). It uses DuckDB so you can scaffold and run it locally end-to-end:

```bash
bruin init        # then pick "variant-example" from the list
```

Or skip the picker by passing the template name positionally:

```bash
bruin init variant-example
```

Directory layout:

```diff
variant-example/
+ ├─ pipeline.yml             # variables + variants + templated identity fields
+ ├─ .bruin.yml               # DuckDB connection config
  └─ assets/
    ├─ seed.py                # creates 3 regional schemas + sample users
    ├─ requirements.txt       # duckdb
    ├─ raw_users.sql          # filters source by tenant
    └─ users_summary.sql      # aggregates within forecast_days window
```

Three variants are declared (`client_alpha`, `client_beta`, `client_gamma`) — each pins a different `client`, `region`, `schedule`, and (for two of them) `forecast_days`.

### Trying It Locally — End-to-End

Scaffold the template into a fresh directory, then run each variant:

```bash
cd variant-example

bruin run --variant client_alpha   # alpha_us_pipeline → @hourly, 7-day window
bruin run --variant client_beta    # beta_eu_pipeline  → daily 06:00, 60-day window
bruin run --variant client_gamma   # gamma_ap_pipeline → @weekly, 30-day window (default)
```

Or, if you're using the [Bruin VS Code extension](https://marketplace.visualstudio.com/items?itemName=bruin.bruin), open any asset in `variant-example/assets/` — the asset panel shows a **Variant** dropdown next to the **Environment** selector once the pipeline declares variants. Pick a variant and hit **Run** to execute it.

Inspect the results:

```bash
duckdb /tmp/variants_demo.duckdb <<'SQL'
SELECT 'alpha' AS variant, * FROM alpha_users_summary_us
UNION ALL SELECT 'beta',  * FROM beta_users_summary_eu
UNION ALL SELECT 'gamma', * FROM gamma_users_summary_ap;
SQL
```
