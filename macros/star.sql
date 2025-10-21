{# 
Cross-platform implementation of Snowflake's EXCLUDE and RENAME functionality

Arguments:
    from: The table to select from
    except: A list of columns to exclude
    rename: A paired list of columns to rename ([old_name, new_name])

Notes:
    This macro is more complex than it otherwise would be because Databricks does
    not support RENAME in select statements.

    The Databricks implementation does not retain the original column order.
    
    SQL Server implementation uses INFORMATION_SCHEMA for reliable column discovery
    and explicit column listing since SQL Server doesn't support EXCLUDE syntax.
#}

{% macro star(from, except=[], rename=[], prefix=None, suffix=None, quote_identifiers=None, upper=None, relation=None, relations=None) %}
    {{ return(adapter.dispatch('star', 'edu_edfi_source')(from, except, rename, prefix, suffix, quote_identifiers, upper, relation, relations)) }}
{% endmacro %}

{% macro snowflake__star(from, except, rename) -%}

    {{from}}.* 
    {% if except | length > 0 -%}
    exclude ({{ except | join(',') }})
    {% endif %}
    {% if rename | length > 0 -%} 
    rename (
    {% for col_pair in rename -%}
        {{ col_pair[0] | trim }} as {{ col_pair[1] | trim }} {% if not loop.last %},{% else %}){% endif %}
    {% endfor -%}
    {% endif %}

{%- endmacro %}


{% macro databricks__star(from, except, rename) -%}

    {% for col_pair in rename -%}
    {{ col_pair[0] | trim }} as {{ col_pair[1] | trim }},
    {{ except.append(col_pair[0] | trim) or "" }}
    {% endfor -%}
    {{from}}.*
    {% if except | length > 0 -%} 
    except ({{ except | join(', ') }})
    {% endif -%}

{%- endmacro %}

{% macro sqlserver__star(from, except=[], rename=[], prefix=None, suffix=None, quote_identifiers=None, upper=None, relation=None, relations=None) -%}
    
    {#-- Prevent querying of db in parsing mode --#}
    {%- if not execute -%}
        {% do return('*') %}
    {%- endif -%}

    {#-- Parse the from relation to get schema and table info --#}
    {%- if from is string -%}
        {%- set from_parts = from.split('.') -%}
        {%- if from_parts | length == 3 -%}
            {%- set schema_name = from_parts[1] -%}
            {%- set table_name = from_parts[2] -%}
            {%- set database_name = from_parts[0] -%}
        {%- elif from_parts | length == 2 -%}
            {%- set schema_name = from_parts[0] -%}
            {%- set table_name = from_parts[1] -%}
            {%- set database_name = target.database -%}
        {%- else -%}
            {%- set schema_name = target.schema -%}
            {%- set table_name = from_parts[0] -%}
            {%- set database_name = target.database -%}
        {%- endif -%}
    {%- else -%}
        {%- set schema_name = from.schema or target.schema -%}
        {%- set table_name = from.identifier -%}
        {%- set database_name = from.database or target.database -%}
    {%- endif -%}

    {#-- Build excluded and renamed column lists for SQL Server --#}
    {%- set excluded_cols = [] -%}
    {%- for col in except -%}
        {%- do excluded_cols.append(col.upper()) -%}
    {%- endfor -%}
    
    {%- set rename_dict = {} -%}
    {%- for col_pair in rename -%}
        {%- do rename_dict.update({col_pair[0].upper(): col_pair[1]}) -%}
        {%- do excluded_cols.append(col_pair[0].upper()) -%}
    {%- endfor -%}

    {#-- Get columns using SQL Server INFORMATION_SCHEMA --#}
    {% set get_columns_sql %}
        SELECT 
            COLUMN_NAME,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE 
            TABLE_SCHEMA = '{{ schema_name }}'
            AND TABLE_NAME = '{{ table_name }}'
            {% if database_name %}
            AND TABLE_CATALOG = '{{ database_name }}'
            {% endif %}
            {% if excluded_cols | length > 0 %}
            AND UPPER(COLUMN_NAME) NOT IN (
                {%- for col in excluded_cols -%}
                    '{{ col }}'
                    {%- if not loop.last -%},{%- endif -%}
                {%- endfor -%}
            )
            {% endif %}
        ORDER BY ORDINAL_POSITION
    {% endset %}

    {%- if execute -%}
        {%- set results = run_query(get_columns_sql) -%}
        {%- if results.rows | length > 0 -%}
            {%- set cols = results.columns[0].values() -%}
        {%- else -%}
            {%- set cols = [] -%}
        {%- endif -%}
    {%- else -%}
        {%- set cols = [] -%}
    {%- endif -%}

    {#-- Build the column list with prefixes, suffixes, and renames --#}
    {%- if cols | length <= 0 -%}
        {% if flags.WHICH == 'compile' %}
            {% do return('*') %}
        {% else %}
            {% do return("/* no columns returned from SQL Server star() macro */") %}
        {% endif %}
    {%- else -%}
        {#-- First output renamed columns --#}
        {%- for col_pair in rename -%}
            {%- set original_col = col_pair[0] | trim -%}
            {%- set new_col = col_pair[1] | trim -%}
            {%- if quote_identifiers -%}
                [{{ original_col }}] AS [{{ prefix or '' }}{{ new_col }}{{ suffix or '' }}]
            {%- else -%}
                {{ original_col }} AS {{ prefix or '' }}{{ new_col }}{{ suffix or '' }}
            {%- endif -%}
            {%- if not loop.last or cols | length > 0 -%},{%- endif -%}
            {{ '\n  ' }}
        {%- endfor -%}
        
        {#-- Then output regular columns --#}
        {%- for col in cols -%}
            {%- set col_name = col | trim -%}
            {%- if quote_identifiers -%}
                [{{ col_name }}]
                {%- if prefix or suffix -%} AS [{{ prefix or '' }}{{ col_name }}{{ suffix or '' }}]{%- endif -%}
            {%- else -%}
                {{ col_name }}
                {%- if prefix or suffix -%} AS {{ prefix or '' }}{{ col_name }}{{ suffix or '' }}{%- endif -%}
            {%- endif -%}
            {%- if not loop.last -%},{%- endif -%}
            {%- if not loop.last -%}{{ '\n  ' }}{%- endif -%}
        {%- endfor -%}
    {%- endif -%}

{%- endmacro %}