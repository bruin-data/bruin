""" @bruin
name: basic
type: python
columns:
    - name: date
      checks:
        - name: not_null
    - name: wind
      checks:
        - name: not_null

custom_checks:
    - name: check1
      query:
        SELECT COUNT(DISTINCT power_plant_id)
        FROM earth_external.epias_power_generation
        WHERE loaded_at = (
            SELECT MAX(loaded_at)
            FROM `earth_external.epias_power_generation`
        )
      value: 12
@bruin """

import json
import os

print("Bruin says hello!")
print("Here are some global variables: ")
print("  - BRUIN_START_DATE: ", os.getenv('BRUIN_START_DATE'))
print("  - BRUIN_RUN_ID: ", os.getenv('BRUIN_RUN_ID'))
print("We are done here.")
