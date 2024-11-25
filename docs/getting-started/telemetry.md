---
outline: deep
---

# Telemetry
bruin uses a very basic form of **anonymous telemetry** to be able to keep track of the usage on a high-level.
- It uses anonymous machine IDs that are hashed to keep track of the number of unique users.
- It sends the following information:
    - bruin version
    - machine ID (Anomymous)
    - OS info: OS, architecture
    - command executed
    - success/failure
    - Stats on types of assets

The information collected here is used to understand the usage of bruin and to improve the product. We use [Rudderstack](https://www.rudderstack.com/) to collect the events and we do not store any PII.

The questions we answer with this information are:
- How many unique users are using bruin?
- How many times is bruin being used and in what way?

## Disabling Telemetry
If you'd like to disable telemetry, simply set the `TELEMETRY_OPTOUT` environment variable to `true`.