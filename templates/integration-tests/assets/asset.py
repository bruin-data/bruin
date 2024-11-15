""" @bruin

name: python_asset
image: python:3.11
depends:
    - chess_playground.player_summary

secrets:
    - key: KEY1
      injected_as: INJECTED1

@bruin """

import os
if os.getenv('INJECTED1') != "value1":
    raise Exception("KEY1 is not injected correctly")
