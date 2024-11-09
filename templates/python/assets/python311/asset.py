""" @bruin

name: myschema.my_python_asset_311
image: python:3.11

description: |
  # Sample Python asset
  This file will be executed as is, it can import other Python modules, install any packages, etc.

  You can define columns and custom checks that can be executed in the same way as SQL assets, as long as the asset name matches the table name.
  
  - For the dependencies to be installed, Bruin will find the closes requirements.txt file and install the dependencies there in isolated environments.
  - Bruin will execute the script, and then run all the quality checks afterwards.

@bruin """

import cowsay 

cowsay.cow('Hello World')

import sys
print("=================")
print(sys.version)
print("=================")