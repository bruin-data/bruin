"""@bruin

name: python_asset_happy

depends:
  - products
image: python:3.11

secrets:
  - key: KEY1
    inject_as: INJECTED1
  - key: chess-default
    inject_as: chess-default

@bruin"""

import os

if os.getenv('INJECTED1') != "value1":
    raise Exception("KEY1 is not injected correctly")

if os.getenv('chess-default') != '{"name":"chess-default","players":["erik","vadimer2"]}':
    print( os.getenv('chess-default'))
    raise Exception("chess-default is not injected correctly")
