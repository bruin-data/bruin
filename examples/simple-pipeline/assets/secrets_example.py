# @bruin.name: secrets_example
# @bruin.type: python
# @bruin.secrets: KEY1:INJECTED1, KEY2_AS_IS
import os

print("Bruin says hello!")

print("Key1 is injected as INJECTED1, its value is: ", os.getenv('INJECTED1'))
print("Key2 is injected as KEY2_AS_IS, its value is: ", os.getenv('KEY2_AS_IS'))

print("We are done here.")
