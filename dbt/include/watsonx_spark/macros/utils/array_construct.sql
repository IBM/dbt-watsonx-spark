{% macro watsonx_spark__array_construct(inputs, data_type) -%}
    array( {{ inputs|join(' , ') }} )
{%- endmacro %}
