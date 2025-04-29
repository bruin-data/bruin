/* @bruin
name: non_compliant.tertiary
type: bq.sql
description: |
    This pipeline demonstrates the use of non-compliant assets
columns:
    - name: name
      type: string
    - name: age
      type: string
    - name: country
      type: string
@bruin */

select 
    "Jhon" as name,
    "21" as age,
    "USA" as country