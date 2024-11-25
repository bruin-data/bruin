""" @bruin

name: myschema.my_python_asset_311
image: python:3.11

description: |
  This file will be executed as is, it can import other Python modules, install any packages, etc.

  For the dependencies to be installed, Bruin will find the closes requirements.txt file and install the dependencies there in isolated environments.

@bruin """

import platform
import cowsay

cowsay.cow(f"Python version: {platform.python_version()}")