""" @bruin
name: variables.py
description: integration test for variables in python assets
@bruin"""

import os
import json

vars = json.loads(os.environ.get("BRUIN_VARS"))

print(f"env: {vars['env']}")
print(f"users: {','.join(vars['users'])}")
