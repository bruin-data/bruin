/* @bruin
name: casefile.teardown
description: |
  Removes the generation scaffolding. Everything the town was built from lives in
  the _gen schema, and none of it belongs in the database you play against, so the
  last step of the run drops the whole schema.

  This is its own asset on purpose. Excluding it with
  `bruin run --exclude-tag scaffolding` leaves the scaffolding in place, which is
  how the template's own test suite checks that the case still has exactly one
  answer. Running it that way on your own copy will spoil the case for you.
tags:
  - scaffolding
depends:
  - town.citizens
  - town.addresses
  - town.cell_towers
  - town.cameras
  - town.vehicles
  - town.vehicle_insurance
  - town.devices
  - town.device_pings
  - town.call_records
  - town.card_transactions
  - town.plate_reads
  - town.firearm_licences
  - town.range_visits
  - town.businesses
  - town.employment
  - town.badges
  - town.building_readers
  - town.building_access_events
  - town.bank_accounts
  - town.merchants
  - town.property_records
  - town.council_decisions
  - town.travel_records
  - town.hotel_stays
  - town.clinic_visits
  - town.library_loans
  - town.gym_checkins
  - town.parking_citations
  - casefile.witness_statements
  - casefile.forensic_findings
  - casefile.interview_notes
@bruin */

DROP SCHEMA IF EXISTS _gen CASCADE
