""" @bruin

name: pyproject_dep_check
image: python:3.11

@bruin """

import pydantic

print(f"pydantic version: {pydantic.__version__}")
print("pyproject.toml dependency resolution works!")
