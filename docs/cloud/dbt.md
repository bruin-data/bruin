# DBT pipelines

Bruin Cloud is able to run [DBT](https://www.getdbt.com/) pipelines. In Bruin Cloud the DBT models will be represented as Bruin Assets.
You can define a schedule to run the DBT pipeline but adding a special variable in your `dbt_project.yml` file. E.g:

```yaml
vars:
  bruin_schedule: "*/5 0 0 0 0"
```
