""" @bruin

name: python_asset_happy
image: python:3.11
depends:
    - products

secrets:
    - key: KEY1
      inject_as: INJECTED1  
    - key: chess-default

@bruin """

import os

if os.getenv('INJECTED1') != "value1":
    raise Exception("KEY1 is not injected correctly")

if os.getenv('chess-default') != '{"name":"chess-default","players":["MagnusCarlsen","Hikaru"]}':
    print( os.getenv('chess-default'))
    raise Exception("chess-default is not injected correctly")


