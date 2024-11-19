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

con = duckdb.connect(database = "duckdb.db", read_only = False)

con.execute("SELECT * FROM chess_playground.player_summary")
result = con.fetchall()
if len(result) != 2:
    raise Exception("Incorrect number of rows in player_summary")
