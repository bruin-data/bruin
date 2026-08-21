{#
  Fixed dates and window bounds for the Ashmont datasets.

  Every timestamp in the town is naive local time in a single fixed zone with no
  daylight saving, so no clock arithmetic anywhere can produce a gap, a repeat or
  an off-by-an-hour that looks like a clue.
#}

{# The rally. #}
{% macro rally_date() -%}DATE '2026-05-14'{%- endmacro %}
{% macro rally_shot_ts() -%}TIMESTAMP '2026-05-14 18:47:00'{%- endmacro %}

{#
  Movement data — device pings, plate reads, badge swipes — ends the week after
  the rally and runs back `ping_window_days`. Everything derived from this macro
  moves together when that variable changes.
#}
{% macro movement_days() -%}{{ var.ping_window_days | int }}{%- endmacro %}
{% macro movement_end() -%}DATE '2026-05-21'{%- endmacro %}
{% macro movement_start() -%}(DATE '2026-05-22' - INTERVAL {{ var.ping_window_days | int }} DAY)::DATE{%- endmacro %}

{# Financial and telephony history runs 90 days, long enough for a pattern to form. #}
{% macro ledger_days() -%}91{%- endmacro %}
{% macro ledger_start() -%}DATE '2026-02-20'{%- endmacro %}
{% macro ledger_end() -%}DATE '2026-05-21'{%- endmacro %}
