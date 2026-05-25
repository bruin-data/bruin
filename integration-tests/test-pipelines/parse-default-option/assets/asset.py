""" @bruin

name: python_asset
image: python:3.11
depends:
    - chess_playground.player_summary

secrets:
    - key: KEY1
      inject_as: INJECTED1
    
@bruin """

import os
import duckdb

if os.getenv('INJECTED1') != "value1":
    raise Exception("KEY1 is not injected correctly")

con = duckdb.connect(database = "duckdb-files/env-run-default-option.db", read_only = False)

con.execute("SELECT COUNT(*) FROM chess_playground.player_summary")
result = con.fetchone()
if result[0] == 0:
    raise Exception("player_summary is empty")
