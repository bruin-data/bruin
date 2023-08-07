# @bruin.name: secrets_example
# @bruin.type: python
# @bruin.secrets: KEY1:INJECTED1, KEY2_AS_IS
# @bruin.secrets: snowflake
import json
import os

print("Bruin says hello!")

print("KEY1 is injected as INJECTED1, its value is:", os.getenv('INJECTED1'))
print("KEY2_AS_IS is injected as is, its value is:", os.getenv('KEY2_AS_IS'))

gcpConnection = os.getenv('snowflake')
print("snowflake is injected from a connection, its value is:", gcpConnection)

gcpConnAsDict = json.loads(gcpConnection)
print(gcpConnAsDict)

print("We are done here.")
