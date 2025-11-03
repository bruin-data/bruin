/* @bruin

name: public.alternative
type: duckdb.sql
description: |
  This pipeline demonstrates the use of the standard policy.
owner: engineering@getbruin.com

columns:
  - name: msgOfTheDay

custom_checks:
  - name: proof of concept
    value: 0
    query: select true

@bruin */

select "easy comes easy goes" as MSGOFTHEDAY
