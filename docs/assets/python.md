# Python Assets

Bruin takes the Python data development experience to the next level:
- Bruin runs assets in isolated environments: mix and match Python versions & dependencies
- It installs & manages Python versions automatically, so you don't have to have anything installed
- You can return dataframes and it uploads them to your destination
- You can run quality checks on it just as a regular asset

Bruin uses the amazing [`uv`](https://astral.sh/uv) under the hood to abstract away all the complexity.

Python assets are built to be as flexible as possible. You can use any Python package you want, as long as it is installable with `pip`.

```bruin-python
"""@bruin
name: tier1.my_custom_api
image: python:3.13
connection: bigquery

materialization:
  type: table
  strategy: merge

columns:
  - name: col1
    type: integer
    checks:
      - name: unique
      - name: not_null
@bruin"""

import pandas as pd

def materialize():
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
```

## Dependency resolution
Python assets are searching for the closest `requirements.txt` file in the file tree and creates a virtual environment for that file.

For example, assume you have a file tree such as:
```
* folder1/
    * folder2/
        * test.py
        * requirements.txt
    * folder3/
        * test2.py
    * requirements.txt
* folder4/
    * folder5/
        * folder6/
            * test3.py
* requirements.txt
```

* When Bruin runs `test.py`, it will use the `folder1/folder2/requirements.txt` in `folder2`, since they are in the same folder.  
* For `test2.py`, since there is no `requirements.txt` in the same folder, Bruin goes up one level in the tree and finds `folder1/requirements.txt`.  
* Similarly, `requirements.txt` in the main folder used for `test3.py` since none of `folder6`, `folder5` and `folder4` have any `requirements.txt` files.

## Python versions
Bruin supports various Python versions in the same pipeline, all running in isolated environments. The resolved dependencies will be installed correctly for the corresponding Python version without impacting each other.

You can define Python versions using the `image` key:
```bruin-python
"""@bruin
name: tier1.my_custom_api
image: python:3.11
@bruin"""

print('hello world')
```

## Environment variables
Bruin introduces a set of environment variables by default to every Python asset.

The following environment variables are available in every Python asset execution:

* `BRUIN_START_DATE`: The start date of the pipeline run in `YYYY-MM-DD` format (e.g. `2024-01-15`)
* `BRUIN_START_DATETIME`: The start date and time of the pipeline run in `YYYY-MM-DDThh:mm:ss` format (e.g. `2024-01-15T13:45:30`)
* `BRUIN_START_TIMESTAMP`: The start timestamp of the pipeline run in RFC3339 format with timezone (e.g. `2024-01-15T13:45:30.000000Z07:00`)
* `BRUIN_END_DATE`: The end date of the pipeline run in `YYYY-MM-DD` format (e.g. `2024-01-15`)
* `BRUIN_END_DATETIME`: The end date and time of the pipeline run in `YYYY-MM-DDThh:mm:ss` format (e.g. `2024-01-15T13:45:30`) 
* `BRUIN_END_TIMESTAMP`: The end timestamp of the pipeline run in RFC3339 format with timezone (e.g. `2024-01-15T13:45:30.000000Z07:00`)
* `BRUIN_RUN_ID`: The unique identifier for the pipeline run
* `BRUIN_PIPELINE`: The name of the pipeline being executed
* `BRUIN_FULL_REFRESH`: Set to `1` when the pipeline is running with the `--full-refresh` flag, empty otherwise



## Materialization - Beta

Bruin runs regular Python scripts by default; however, quite often teams need to load data into a destination from their Python scripts. Bruin supports materializing the data returned by a Python script into a data warehouse.

The requirements to get this working is:
- define a `materialization` config in the asset definition
- have a function called `materialize` in your Python script that returns a pandas/polars dataframe or a list of dicts.

> [!WARNING]
> This feature has been very recently introduced, and is not battle-tested yet. Please create an issue if you encounter any bugs.

```bruin-python
"""@bruin
name: tier1.my_custom_api
image: python:3.13
connection: bigquery

materialization:
  type: table
  strategy: merge
 
columns:
    - name: col1
      primary_key: true
@bruin"""

import pandas as pd

def materialize():
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
```

### Under the hood

Bruin uses Apache Arrow under the hood to keep the returned data efficiently, and uses [ingestr](https://github.com/bruin-data/ingestr) to upload the data to the destination. The workflow goes like this:
- install the asset dependencies using `uv`
- run the `materialize` function of the asset
- save the returned data into a temporary file using Arrow memory-mapped files
- run ingestr to load the Arrow memory-mapped file into the destination
- delete the memory-mapped file

This flow ensures that the typing information gathered from the dataframe will be preserved when loading to the destination, and it supports incremental loads, deduplication, and all the other features of ingestr.

## Column-level lineage

Bruin supports column-level lineage for Python assets as well as SQL assets. In order to get column-level lineage, you need to annotate the columns that are exposed by the Bruin asset.

```bruin-python
""" @bruin
name: myschema.my_mat_asset 
materialization:
  type: table
  strategy: merge

columns:
    - name: col1
      type: int
      upstreams:       # [!code ++]
        - table: xyz   # [!code ++]
          column: col1 # [!code ++]

@bruin """

import pandas as pd

def materialize():
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
```

Bruin will use the annotations to build the column-lineage dependency across all of your assets, including those extracted from SQL automatically.


## Examples
### Print hello world
```bruin-python
""" @bruin
name: hello_world
@bruin """

print("Hello World!")
```

### Ingest data to BigQuery via an API manually
```bruin-python
""" @bruin
name: raw_data.currency_rates
type: python
parameters:
    loader_file_format: jsonl
secrets:
    - key: bigquery_conn
@bruin """

import os
import currency_rates
import pandas as pd
import json
from google.cloud import bigquery

# Bruin injects secrets as a JSON string.
# This function takes a connection name and returns a BigQuery client
def get_bq_client(conn_name: str) -> bigquery.Client:
    serv_acc = json.loads(os.environ[conn_name])
    return bigquery.Client.from_service_account_info(
        json.loads(serv_acc["service_account_json"]), 
        project=serv_acc["project_id"]
    )

START_DATE = os.environ["BRUIN_START_DATE"]
END_DATE = os.environ["BRUIN_END_DATE"]

bq_client = get_bq_client("bigquery_conn")
df = currency_rates.get_rates(start=START_DATE, end=END_DATE)

df.to_gbq("raw_data.currency_rates", if_exists="replace", credentials=bq_client._credentials)
```
