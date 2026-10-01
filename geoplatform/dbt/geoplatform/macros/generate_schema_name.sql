{#
    Use the custom schema name verbatim (e.g. `staging`, `marts`) instead of
    dbt's default behaviour of prefixing it with the target schema
    (which would produce `dbt_geoplatform_marts`). The serving API queries
    these schemas by their bare names (e.g. `marts.satellite_observation`),
    so the physical schema must match exactly.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
