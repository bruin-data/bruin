# Python Assets
Bruin supports running Python natively, wherever you are running Bruin.

- It spins up an individual virtual environment for each Python asset.
- It passes secrets as environment variables.
- It injects the start and end dates as environment variables.
- It caches the virtual environments for faster execution.

Python assets are built to be as flexible as possible. You can use any Python package you want, as long as it is installable with `pip`.

### Python Packages - Requirements
Python assets are searching for the closest `requirements.txt` file in the file tree and creates a virtual environment for that file.

For example, assume you have a file tree such as:
```
* folder1
    * folder2
        * test.py
        * requirements.txt
    * folder3
        * test2.py
    * requirements.txt
* folder4
    * folder5
        * folder6
            * test3.py
* requirements.txt
```

* When Bruin runs `test.py`, it will use the `folder1/folder2/requirements.txt` in `folder2`, since they are in the same folder.  
* For `test2.py`, since there is no `requirements.txt` in the same folder, Bruin goes up one level in the tree and finds `folder1/requirements.txt`.  
* Similarly, `requirements.txt` in the main folder used for `test3.py` since none of `folder6`, `folder5` and `folder4` have any `requirements.txt` files.

Each virtual environment is cached in `~/.bruin/virtualenvs` folder

### Examples
#### Print hello world
```bruin-python
""" @bruin
name: hello_world
type: python
@bruin """

print("Hello World!")
```

#### Ingest data to BigQuery via an API
```bruin-python
""" @bruin
name: raw_data.currency_rates
type: python
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
