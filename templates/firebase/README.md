# Bruin - Firebase to GCP Pipeline

This pipeline is a simple example of a Bruin pipeline that transfers data from Firebase to Bigquery. 

The pipeline includes several sample assets:

- `events/events.asset.yaml`: Monitors for new events data in BigQuery to trigger downstream tasks when new data is detected.
- `events/events.sql`: Defines a BigQuery view for formatted Firebase Analytics event data to support ad-hoc analysis.
- `fn/date_in_range.sql`: A function asset that checks if a date is within a specified range.
- `fn/get_params_to_json.sql`: A function asset that converts parameter data to JSON format.
- `fn/get_param_bool.sql`, `fn/get_param_double.sql`, `fn/get_param_int.sql`, `fn/get_param_str.sql`: SQL assets that retrieve specific types of parameters (boolean, double, integer, string).
- `fn/parse_version.sql`: A function asset for parsing version information from a string.
- `fn/user_properties_to_json.sql`: A function asset that converts user properties into JSON format, excluding certain fields.
- `user_model/cohorts.sql`: A SQL asset that defines cohort-based aggregations for user data.
- `user_model/users.sql`: A SQL asset that defines the users table structure.
- `user_model/users_daily.sql`: A SQL asset that manages daily updates for user data.

## Setup
The pipeline includes a `.bruin.yml` file where you need to configure your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` configuration:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-id"
