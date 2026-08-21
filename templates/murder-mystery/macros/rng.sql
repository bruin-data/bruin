{#
  Deterministic value derivation for the Yorkville datasets.

  Every generated value comes from an MD5 digest of the row's own identity plus a
  per-column salt. Nothing depends on row order, on how many rows came before, or
  on a session seed, so:

    - the same install always produces the same town;
    - adding a row to one table never reshuffles another;
    - two columns of the same row are uncorrelated, because their salts differ.

  MD5 is used rather than DuckDB's hash() because MD5 is a fixed published
  algorithm: its output cannot drift when DuckDB is upgraded. hash() carries no
  such guarantee, and a drifting hash would silently invalidate a player's notes.

  `key`  is any SQL expression that uniquely identifies the row.
  `salt` is a fixed integer literal, unique per column. Every salt is combined
         with the `town_seed` pipeline variable, so changing that one variable
         regenerates the whole town.
#}

{% macro rnd(key, salt) -%}
((md5_number_lower(({{ key }})::VARCHAR || '|{{ (salt | int) + (var.town_seed | int) }}') % 1000000000)::DOUBLE / 1000000000.0)
{%- endmacro %}


{# An integer drawn uniformly from [lo, hi] inclusive. #}
{% macro rnd_int(key, salt, lo, hi) -%}
({{ lo }} + floor({{ rnd(key, salt) }} * {{ (hi | int) - (lo | int) + 1 }})::BIGINT)
{%- endmacro %}


{# True with probability p. #}
{% macro chance(key, salt, p) -%}
({{ rnd(key, salt) }} < {{ p }})
{%- endmacro %}


{# One element of `items`, drawn uniformly. #}
{% macro pick(key, salt, items) -%}
list_extract([{% for it in items %}'{{ it }}'{% if not loop.last %}, {% endif %}{% endfor %}], 1 + floor({{ rnd(key, salt) }} * {{ items | length }})::BIGINT)
{%- endmacro %}


{#
  One element of `choices`, drawn against declared weights.

  `choices` is a list of [value, cumulative_percent] pairs in ascending order,
  with the last entry at 100 — so a distribution is declared as data rather than
  buried in a CASE ladder. The pairs are expanded into a 100-slot lookup list,
  which keeps the cost at one digest per row however many categories there are.
#}
{% macro weighted(key, salt, choices) -%}
list_extract([{% for i in range(100) %}{% for c in choices %}{% if i < c[1] and (loop.first or i >= choices[loop.index0 - 1][1]) %}'{{ c[0] }}'{% endif %}{% endfor %}{% if not loop.last %}, {% endif %}{% endfor %}], 1 + floor({{ rnd(key, salt) }} * 100)::BIGINT)
{%- endmacro %}


{#
  A bell-shaped draw on [0,1), mean 0.5. Three uniform draws averaged: enough of
  a central tendency for body measurements and scores, where a flat distribution
  reads as obviously synthetic. Consumes salts `salt`, `salt`+1 and `salt`+2.
#}
{% macro rnd_bell(key, salt) -%}
(({{ rnd(key, salt) }} + {{ rnd(key, (salt | int) + 1) }} + {{ rnd(key, (salt | int) + 2) }}) / 3.0)
{%- endmacro %}


{#
  A standard normal draw, by the Box-Muller transform on two uniform draws.
  Consumes salts `salt` and `salt`+1.

  Body measurements need genuine normal tails: a triangular draw is bounded, and
  the bound lands exactly where the interesting part of the distribution is. The
  first uniform is floored away from zero because ln(0) is undefined.
#}
{% macro rnd_norm(key, salt) -%}
(sqrt(-2.0 * ln(greatest({{ rnd(key, salt) }}, 0.000000001))) * cos(2.0 * pi() * {{ rnd(key, (salt | int) + 1) }}))
{%- endmacro %}
