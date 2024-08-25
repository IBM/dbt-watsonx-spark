{% macro watsonx_spark__any_value(expression) -%}
    {#-- return any value (non-deterministic)  --#}
    first({{ expression }})

{%- endmacro %}
