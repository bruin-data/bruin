""" @bruin
name: secrets_example
secrets:
    - key: KEY1
      inject_as: INJECTED1
    - key: KEY2_AS_IS
    - key: snowflake
@bruin """
import json
import os

print("Bruin says hello!")

print("KEY1 is injected as INJECTED1, its value is:", os.getenv('INJECTED1'))
print("KEY2_AS_IS is injected as is, its value is:", os.getenv('KEY2_AS_IS'))

gcpConnection = os.getenv('snowflake')
print("snowflake is injected from a connection, its value is:", gcpConnection)

gcpConnAsDict = json.loads(gcpConnection)
print(gcpConnAsDict)

print("Also some global variables: ", os.getenv('BRUIN_START_DATE'), os.getenv('BRUIN_RUN_ID'))
print("We are done here.")
