# dbt Projects

Bruin Cloud supports [dbt](https://www.getdbt.com/) projects natively, where your dbt models will be represented as assets in Bruin Cloud.

You can define a schedule to run your dbt pipeline by adding a special variable in your `dbt_project.yml` file:

```yaml
vars:
  bruin_schedule: "*/5 0 0 0 0"
```

You can define any schedule you want using the cron syntax. The above example will run your dbt pipeline every 5 minutes.

> [!NOTE]
> The `bruin_schedule` variable is optional. If you don't define it, your dbt pipeline will not run automatically.

