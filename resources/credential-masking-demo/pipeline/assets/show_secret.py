"""@bruin

name: show_secret

secrets:
  - key: demo-api
    inject_as: DEMO_API_TOKEN

@bruin"""

import base64
import os
import urllib.parse


token = os.environ["DEMO_API_TOKEN"]

print(f"raw token:     {token}")
print(f"URL-encoded:   {urllib.parse.quote(token)}")
print(f"base64 token:  {base64.b64encode(token.encode()).decode()}")
