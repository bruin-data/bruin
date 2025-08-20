## Using .bruin.yml as a Secrets Backend

The easiest secret backend you can use with Bruin is a `.bruin.yml` file. This file should sit at the root of the repository and will contain the connection details and other secrets used in your pipelines.

`.bruin.yml` is the "default" secrets backend, so in order to use you don't need to do anything, just have the file at the root of your project.
You can also have the file at a different location and use the flag `--config-file /path/to/.bruin.yml` 

## `.bruin.yml` Schema

.bruin.yml contains information about your secrets and credentials organised in "environments". It also contains information about the default environment to use when none is specified. The environments containe a set of cconnections grouped by platform. This is an example that illustrates the schema from the [Quickstart](../getting-started/introduction/quickstart.md)

```
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
      chess:
        - name: "chess-default"
          players:
            - "erik"
            - "vadimer2"
```