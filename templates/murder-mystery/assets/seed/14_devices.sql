/* @bruin
name: town.devices
description: |
  Handsets active on the Ashmont networks. Prepaid handsets are sold over the
  counter and carry no subscriber record, so their citizen_id is null; around
  fourteen hundred residents use one as their only phone.
materialization:
  type: table
depends:
  - _gen.device_plan
columns:
  - name: device_id
    type: varchar
    description: Handset identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: msisdn
    type: varchar
    description: Subscriber number, format 55-###-####
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Registered subscriber, null for prepaid handsets
  - name: activated_date
    type: date
    description: Date the handset first appeared on the network
  - name: handset_model
    type: varchar
    description: Handset make and model
@bruin */

SELECT
    device_id,
    msisdn,
    citizen_id,
    activated_date,
    handset_model
FROM _gen.device_plan
