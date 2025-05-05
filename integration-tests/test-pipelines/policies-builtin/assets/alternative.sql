/* @bruin
name: public.alternative
type: bq.sql
description: |
    This pipeline demonstrates the use of the standard policy.
owner: engineering@getbruin.com
columns:
    - name: msgOfTheDay

custom_checks:
    - name: proof of concept
      query: select true
@bruin */

select "easy comes easy goes" as msgOfTheDay