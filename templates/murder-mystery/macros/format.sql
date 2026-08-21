{#
  Identifier formats used across the town.

  Ashmont's own conventions, none of them borrowed from a real numbering plan.
#}

{#
  A subscriber number from an integer index.

  The mapping is injective for any index below 9,000,000 because 7,919 is coprime
  with the modulus, so two callers drawing from disjoint index ranges can never
  collide. Residents draw on their own sequence; extra and prepaid handsets draw
  on indices above the resident range.
#}
{% macro msisdn(idx) -%}
('55-' || substr(lpad(((({{ idx }}) * 7919) % 9000000 + 1000000)::VARCHAR, 7, '0'), 1, 3)
       || '-' || substr(lpad(((({{ idx }}) * 7919) % 9000000 + 1000000)::VARCHAR, 7, '0'), 4, 4))
{%- endmacro %}

{% macro cell_id_of(n) -%}
('CELL-' || lpad(({{ n }})::VARCHAR, 3, '0'))
{%- endmacro %}
