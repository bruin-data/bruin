/* @bruin
name: public.standard
type: bq.sql
description: |
    This pipeline demonstrates the use of the standard policy.
owner: engineering@getbruin.com
columns:
  - name: msg
    type: string
    primary_key: true
    description: The contents of the message

tags:
  - layer:raw

custom_checks:
    - name: proof of concept
      query: select true
@bruin */

select "I'm the standard" as msg