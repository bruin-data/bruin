/* @bruin
name: non_compliant.primary
type: bq.sql
description: |
    This pipeline demonstrates the use of non-compliant assets
owner: engineering@getbruin.com
columns:
    - name: name
      type: string
    - name: age
      type: string
@bruin */

select 
    "Jhon" as name,
    "21" as age,
    "USA" as country