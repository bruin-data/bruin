""" @bruin

name: python_asset
image: python:3.11
depends:
    - products

secrets:
    - key: KEY1
      inject_as: INJECTED1  
    - key: chess-default

@bruin """

import os
import duckdb

if os.getenv('INJECTED1') != "value1":
    raise Exception("KEY1 is not injected correctly")

if os.getenv('chess-default') != '{"name":"chess-default","users":["erik","vadimer2"]}':
    print( os.getenv('chess-default'))
    raise Exception("chess-default is not injected correctly")

con = duckdb.connect(database = "duckdb-files/duckdb.db", read_only = False)

con.execute("SELECT * FROM products")
result = con.fetchall()
if len(result) != 4:
    raise Exception("Incorrect number of rows in player_summary")
