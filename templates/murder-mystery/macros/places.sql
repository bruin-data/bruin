{#
  Landmarks the case turns on, as coordinates on the town grid.

  Wychwood Square is open ground on the north side of Austin Terrace. Loma House
  stands on the ridge on the far side of it. The two sit under different cell sites:
  the square is served by the site on the civic steps, the building by an
  operator microcell on its own roof. Dense urban coverage really does look like
  this, and it is why a device on the roof does not disappear into the crowd
  below.
#}

{% macro square_lat() -%}0.029000{%- endmacro %}
{% macro square_lon() -%}0.031000{%- endmacro %}
{% macro square_cell() -%}'CELL-028'{%- endmacro %}

{% macro loma_lat() -%}0.029700{%- endmacro %}
{% macro loma_lon() -%}0.031100{%- endmacro %}
{% macro loma_cell() -%}'CELL-036'{%- endmacro %}

{% macro mews_cell() -%}'CELL-020'{%- endmacro %}
