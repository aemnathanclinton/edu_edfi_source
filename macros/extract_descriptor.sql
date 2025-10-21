{# grab descriptor codes from namespaced descriptor values #}
{% macro extract_descriptor(col,descriptor_name=None) -%}
  {# Determine descriptor key name from the column expression string, if provided in the typical v:fooDescriptor::string form #}
  {%- set stripped_col = (descriptor_name or col.split(":")[-3]) -%}
  {%- set config = var('descriptors', {}).get(stripped_col) or None -%}
  {%- set replace_with = config['replace_with'] if config is not none else none -%}

  {%- if target.type == 'sqlserver' -%}
    {%- set base = jget(col) -%}
    {%- set right_of_hash = "substring(" ~ base ~ ", charindex('#', " ~ base ~ ") + 1, len(" ~ base ~ "))" -%}
    {%- set left_of_hash  = "left(" ~ base ~ ", charindex('#', " ~ base ~ ") - 1)" -%}
    {%- set last_path_seg = "right(namespace, charindex('/', reverse(namespace)) - 1)" -%}
  {%- else -%}
    {# Fallback to dbt_utils on non-sqlserver targets #}
    {%- set right_of_hash = dbt_utils.split_part(col, "'#'", -1) -%}
    {%- set left_of_hash  = dbt_utils.split_part(col, "'#'", 1) -%}
    {%- set last_path_seg = dbt_utils.split_part('namespace', "'/'", -1) -%}
  {%- endif -%}

  {# if not configured to replace (default), return the code portion after '#' #}
  {%- if not replace_with %}
    {{ right_of_hash }}
  {%- else %}
    {# if configured to replace, query int_ef3__deduped_descriptors to find each value's short description #}
    {% set query_descriptors -%}
      select namespace, code_value, {{ replace_with }}
      from {{ ref('int_ef3__deduped_descriptors') }}
      where lower({{ last_path_seg }}) = lower('{{ stripped_col }}')
      order by 1,2
    {%- endset -%}

    {%- set descriptor_xwalk = run_query(query_descriptors) -%}

    {%- if execute -%}
      {%- set namespaces = descriptor_xwalk.columns[0].values() -%}
      {%- set code_values = descriptor_xwalk.columns[1].values() -%}
      {%- set descriptions = descriptor_xwalk.columns[2].values() -%}
    {%- endif -%}

    case
    {%- for i in range(code_values|length) -%}
      when {{ left_of_hash }} = '{{ namespaces[i]|replace("'", "\\'") }}'
       and {{ right_of_hash }} = '{{ code_values[i]|replace("'", "\\'") }}'
        then '{{ descriptions[i]|replace("'", "\\'") }}'
    {%- endfor %}
    else {{ right_of_hash }}
    end
  {%- endif -%}
{%- endmacro %}
