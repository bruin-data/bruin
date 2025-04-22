# Sensor Assets

Sensor assets are a special type of asset that allows you to wait until a certain external event has occurred.

The most common usecase of a sensor is when a piece of data is being produced by external systems, and you want to wait until that data is available before you start processing it. For example, you may want to wait until a file is available in a certain location, or until a certain database table has been populated with data.

Sensors are implemented on a per-platform basis, therefore some platforms may have more sensors than others.

Bruin CLI will run sensors once by default, you can change this behavior by setting the `--sensor-mode` flag to `skip` or `wait`.

## Why?

Sensors allow you to do a couple of things:

- Sensors are useful when you want to wait for an external event to occur before you start processing data.
  - For example, you may want to wait until a file is available in a certain location, or until a certain database table has been populated with data.
- They allow you to enforce quality checks on data that is produced outside of Bruin.
- They allow you to represent external assets within your data lineage and your data catalog.

> [!NOTE]
> Sensors are useful when your pipelines run on a schedule, rather than manual `bruin run` executions. You can control how sensors are executed via the `--sensor-mode` flag in `bruin run` command.

> [!WARNING]
> Bruin CLI supports working sensor implementations for some platforms such as BigQuery or Snowflake, but not all. [Bruin Cloud](https://getbruin.com) supports all sensors, and we are working on adding support for more platforms in Bruin CLI.

Sensors are a crucial part of integrating Bruin with external systems, and we are always happy to improve our sensor coverage.


### Poking Interval

Sensor work as a poking mechanism, they will check the status of the external event every few minutes until the event has occurred.

You can control how often the poking occurs via the `poke_interval` parameter, value being in seconds.

```yaml
name: my_sensor
type: bq.sensor.query
parameters:
    query: select count(*) > 0 from `your-project-id.raw.external_asset`
    poke_interval: 10 # seconds // [!code focus]
```

## Definition Schema

Sensors are defined as YAML files, with the naming schema `<name>.asset.yml`.

They support all the common properties of a Bruin asset, such as documentation, quality checks, and more.

```yaml
name: string
type: string
parameters:
    ... # custom parameters here
```

Here's an example BigQuery SQL sensor:

```yaml
name: raw.external_asset
type: bq.sensor.query
parameters:
    query: select count(*) > 0 from `your-project-id.raw.external_asset`
```

This asset will wait until the query returns any result.





