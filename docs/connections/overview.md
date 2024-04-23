# Connections

Bruin CLI uses a special file called `.bruin.yml` that stores connections and secrets to be used in pipelines. 

When you run a pipeline, Bruin will find this file in the repo root, parse the connections there, build client objects internally to interact with these external platforms and then run your assets.

> [!INFO]
> The first time you run `bruin validate` or `bruin run`, Bruin will create an empty `.bruin.yml` file and add it to `.gitignore` automatically.

Here's an example `.bruin.yml` file:

```yaml
# Name of the default environment when no `--env` flags are given. 
default_environment: default

environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp_connection_name"
          project_id: "project-id"
          service_account_file: "path/to/file.json"

      snowflake:
        - name: "sf_connection_name"
          username: "sfuser"
          password: "XXXXXXXXXX"
          account: "AAAAAAA-AA00000"
          database: "dev"
          schema: "schema_name" # optional
          warehouse: "warehouse_name" # optional
          role: "data_analyst" # optional
          region: "eu-west1" # optional
            
  # An example production environment          
  production:
    connections:
      google_cloud_platform:
        - name: "gcp_connection_name"
          project_id: "prod-project-id"
          service_account_file: "path/to/prod_file.json"

      snowflake:
        - name: "sf_connection_name"
          username: "sfuser-prod"
          password: "XXXXXXXXXX"
          account: "AAAAAAA-AA00000"
          database: "prod"
          schema: "schema_name" # optional
          warehouse: "warehouse_name" # optional
          role: "data_analyst" # optional
          region: "eu-west1" # optional
```

