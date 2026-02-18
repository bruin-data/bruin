""" @bruin

name: nested_pyproject_dep_check
image: python:3.11

@bruin """

import httpx

print(f"httpx version: {httpx.__version__}")
print("nested pyproject.toml dependency resolution works!")
