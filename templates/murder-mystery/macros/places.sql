{#
  Landmarks the case turns on, as coordinates on the town grid.

  Foundry Square is open ground on the north side of Foundry Row. The Corvid
  Building stands on the far side of it. The two sit under different cell sites:
  the square is served by the site on the civic steps, the building by an
  operator microcell on its own roof. Dense urban coverage really does look like
  this, and it is why a device on the roof does not disappear into the crowd
  below.
#}

{% macro square_lat() -%}0.029000{%- endmacro %}
{% macro square_lon() -%}0.031000{%- endmacro %}
{% macro square_cell() -%}'CELL-028'{%- endmacro %}

{% macro corvid_lat() -%}0.029700{%- endmacro %}
{% macro corvid_lon() -%}0.031100{%- endmacro %}
{% macro corvid_cell() -%}'CELL-036'{%- endmacro %}

{% macro kestrel_cell() -%}'CELL-020'{%- endmacro %}
