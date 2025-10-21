{# Cross-platform JSON getter: accepts a Snowflake-style path like "v:foo:bar::string" and emits the right SQL #}
{% macro jget(path_expr) -%}
    {# path_expr should be a string literal like "v:field[:nested]::type" or "v:array" #}
    {%- if target.type == 'sqlserver' -%}
        {%- set expr = path_expr -%}
        {%- if expr.startswith('"') or expr.startswith("'") -%}
            {%- set expr = expr[1:-1] -%}
        {%- endif -%}
        {%- set parts = expr.split('::') -%}
        {%- set left = parts[0] -%}
        {%- set dtype = parts[1] if parts|length > 1 else none -%}
        {%- set tokens = left.split(':') -%}
        {%- set root = tokens[0] -%}
        {%- set path = '$' -%}
        {%- if tokens|length > 1 -%}
            {%- set path = '$.' ~ (tokens[1:] | join('.')) -%}
        {%- endif -%}
        {%- if dtype is none -%}
            json_query({{ root }}, '{{ path }}')
        {%- else -%}
            {%- set lower = dtype|lower -%}
            {%- if lower in ['string', 'variant'] -%}
                try_cast(json_value({{ root }}, '{{ path }}') as nvarchar(max))
            {%- elif lower in ['date'] -%}
                try_cast(json_value({{ root }}, '{{ path }}') as date)
            {%- elif lower in ['timestamp', 'timestamp_ntz', 'datetime'] -%}
                try_cast(json_value({{ root }}, '{{ path }}') as datetime2)
            {%- elif lower in ['time'] -%}
                try_cast(json_value({{ root }}, '{{ path }}') as time)
            {%- elif lower in ['boolean', 'bool'] -%}
                try_cast(case when lower(json_value({{ root }}, '{{ path }}')) in ('true','1') then 1 when lower(json_value({{ root }}, '{{ path }}')) in ('false','0') then 0 else null end as bit)
            {%- elif lower in ['float','double','real'] -%}
                try_cast(json_value({{ root }}, '{{ path }}') as float)
            {%- elif lower in ['int','integer','bigint','smallint','tinyint'] -%}
                try_cast(json_value({{ root }}, '{{ path }}') as bigint)
            {%- elif lower in ['number','decimal','numeric'] -%}
                try_cast(json_value({{ root }}, '{{ path }}') as decimal(38,10))
            {%- else -%}
                json_query({{ root }}, '{{ path }}')
            {%- endif -%}
        {%- endif -%}
    {%- else -%}
        {{ return(path_expr) }}
    {%- endif -%}
{%- endmacro %}
