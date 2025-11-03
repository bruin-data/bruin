/* @bruin

name: public.standard
type: duckdb.sql
description: |
  This pipeline demonstrates the use of the standard policy.
tags:
  - layer:raw
owner: engineering@getbruin.com

columns:
  - name: msg
    type: string
    description: The contents of the message
    primary_key: true

custom_checks:
  - name: proof of concept
    value: 0
    query: select true

@bruin */

select "I'm the standard" as msg
