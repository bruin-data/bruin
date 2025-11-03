/* @bruin

type: bq.sql
description: This function formats an app version string into a standardized three-part version number, each part padded to three digits.

custom_checks:
  - name: parse_version triple digits
    value: 1
    query: SELECT {{ this }}("1.2.3") = "001.002.003"
  - name: parse_version double digits
    value: 1
    query: SELECT {{ this }}("1.2") = "001.002.000"
  - name: parse_version single digits
    value: 1
    query: SELECT {{ this }}("1") = "001.000.000"

@bruin */

CREATE OR REPLACE FUNCTION {{ this }}(app_version STRING) AS
(
    concat(
        substr(concat("00", split(app_version, ".")[safe_offset(0)]), -3),
        ".",
        coalesce(
            substr(concat("00", split(app_version, ".")[safe_offset(1)]), -3),
            "000"
        ),
        ".",
        coalesce(
            substr(concat("00", split(app_version, ".")[safe_offset(2)]), -3),
            "000"
        )
    )
);
