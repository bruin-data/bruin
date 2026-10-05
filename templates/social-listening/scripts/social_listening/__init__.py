"""Shared library for the social-listening Bruin template.

Everything in this package is standard-library Python so the unit tests run
without installing anything. Only ``warehouse.py`` imports the Bruin Python SDK,
and it does so lazily.
"""

TEMPLATE_VERSION = "1.0.0"
