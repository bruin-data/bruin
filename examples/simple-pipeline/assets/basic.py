# @bruin.name: basic

import json
import os

print("Bruin says hello!")
print("Here are some global variables: ")
print("  - BRUIN_START_DATE: ", os.getenv('BRUIN_START_DATE'))
print("  - BRUIN_RUN_ID: ", os.getenv('BRUIN_RUN_ID'))
print("We are done here.")
