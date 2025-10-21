{# SQL Server override for dbt_utils.deduplicate via adapter.dispatch
   Returns a single SELECT (no leading WITH) to be safe inside CTEs.

   Args:
     - relation: name of an already-defined CTE or table to dedupe
     - partition_by: comma-separated column list to define duplicates
     - order_by: ordering expression(s) to choose the "first" row per partition
#}
{% macro sqlserver__deduplicate(relation, partition_by, order_by) -%}
select d.*
from (
  select
    *,
    row_number() over (
      partition by {{ partition_by }}
      order by {{ order_by }}
    ) as __rn
  from {{ relation }}
) as d
where d.__rn = 1
{%- endmacro %}
