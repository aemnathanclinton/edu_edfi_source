{#
Flatten JSON arrays to rows.

Arguments:
    column: column name of array to flatten
    alias: alias assigned to flattened object
    outer: Keep rows with empty lists? Default false.
#}

{% macro json_flatten(column, alias='', outer=False) %}
    {{ return(adapter.dispatch('json_flatten', 'edu_edfi_source')(column, alias, outer)) }}
{% endmacro %}

{% macro sqlserver__json_flatten(column, alias, outer) -%}

 {% if outer %}outer{% else %}cross{% endif %} apply openjson({{ column }}){% if alias != '' %} as {{ alias }}{% endif %}

{%- endmacro %}
{% macro default__json_flatten(column, alias, outer) -%}

{%- endmacro %}

{% macro snowflake__json_flatten(column, alias, outer) -%}

, lateral flatten(input=>{{ column }}, outer=>{{ outer }}) {% if alias != '' %} as {{ alias }} {% endif %}

{%- endmacro %}


{% macro databricks__json_flatten(column, alias, outer) -%}

, lateral variant_explode{% if outer %}_outer{% endif %}({{ column }}) {% if alias != '' %} as {{ alias }} {% endif %}

{%- endmacro %}
