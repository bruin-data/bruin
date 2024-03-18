/* @bruin

name: fn.parse_version
type: bq.sql

custom_checks:
    - name: "parse_version triple digits"
      query: SELECT fn.parse_version("1.2.3") = "001.002.003"
      value: 1
    - name: "parse_version double digits"
      query: SELECT fn.parse_version("1.2") = "001.002.000"
      value: 1
    - name: "parse_version single digits"
      query: SELECT fn.parse_version("1") = "001.000.000"
      value: 1

@bruin */

CREATE OR REPLACE FUNCTION fn.parse_version(app_version STRING) AS 
(
    concat(
        substr(concat("00",split(app_version, ".")[safe_offset(0)]),-3),
        ".",
        coalesce(substr(concat("00",split(app_version, ".")[safe_offset(1)]),-3), "000"),
        ".",
        coalesce(substr(concat("00",split(app_version, ".")[safe_offset(2)]),-3), "000")
    )
);
