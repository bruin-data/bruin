# Google Cloud Platform

In order to have set up a GCP connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. 

Google BigQuery uses a Google Cloud Platform connection type.

```yaml
    connections:
      google_cloud_platform:
        - name: "connection_name"
          project_id: "project-id"
          service_account_file: "path/to/file.json"
```
