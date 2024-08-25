{% materialization view, adapter='watsonx_spark' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
