# @bruin.name: secrets_example
# @bruin.type: python
# @bruin.secrets: KEY1:INJECTED1, KEY2_AS_IS
# @bruin.secrets: KEY3
import os

print("Bruin says hello!")

print("KEY1 is injected as INJECTED1, its value is:", os.getenv('INJECTED1'))
print("KEY2_AS_IS is injected as is, its value is:", os.getenv('KEY2_AS_IS'))
print("KEY3 is injected as is as well, its value is:", os.getenv('KEY3'))

print("We are done here.")
