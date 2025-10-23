{% macro safe_deduplicate(relation, partition_by, order_by, from_relation=none, additional_columns=[], row_number_column='__rn_safe', alias_suffix='safe') %}
    {#- 
        A cross-database deduplication macro that avoids column naming conflicts.
        Uses standard SQL row_number() and dbt's star() macro for portable column selection.
        
        Parameters:
        - relation: The relation (table/CTE) to deduplicate (string name)
        - partition_by: Columns to partition by
        - order_by: Columns to order by  
        - from_relation: Base relation for star() macro (defaults to relation parameter)
        - additional_columns: Extra columns to include that aren't in the base relation (list)
        - row_number_column: Custom name for the row number column (default '__rn_safe')
        - alias_suffix: Suffix for the ranked subquery alias (default 'safe')
    -#}
    
    {%- if from_relation is none -%}
        {%- set from_relation = relation -%}
    {%- endif -%}
    
    select {{ star(from=ref(from_relation)) }}
    {%- if additional_columns -%}
        {%- for col in additional_columns -%}
            , {{ col }}
        {%- endfor -%}
    {%- endif %}
    from (
        select *,
            row_number() over (
                partition by {{ partition_by }}
                order by {{ order_by }}
            ) as {{ row_number_column }}
        from {{ relation }}
    ) as ranked_{{ alias_suffix }}
    where {{ row_number_column }} = 1

{% endmacro %}