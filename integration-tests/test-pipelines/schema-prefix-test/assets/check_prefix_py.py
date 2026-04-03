""" @bruin
name: schema_prefix_test.check_prefix_py
@bruin"""

import os

schema_prefix = os.environ.get("BRUIN_SCHEMA_PREFIX", "")
print(f"schema_prefix: {schema_prefix}")
