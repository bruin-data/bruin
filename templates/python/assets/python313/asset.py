""" @bruin

name: myschema.my_python_asset_313
image: python:3.13

description: |
  This file will be executed as is, it can import other Python modules, install any packages, etc.

  For the dependencies to be installed, Bruin will find the closes requirements.txt file and install the dependencies there in isolated environments.

@bruin """

# we can use pyfiglet because we overrode the default requirements with one that is closer to the asset definition.
# check out the requirements.txt file in the same directory as this file.

import platform
import pyfiglet

print("\nWe can print fancy text with pyfiglet, because we installed it in the requirements.txt file.\n")
print(f"Python version:")
print(pyfiglet.figlet_format(platform.python_version(), font="bubble"))
