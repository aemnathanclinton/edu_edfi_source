-- is this useful?
-- maybe for creating standalone keys, but not for
-- embedding the resolved key into another
{% macro edorg_ref(annualize=False) -%}
  {% set static_cols = ['tenant_code'] %}
  {% if annualize %}
    {% do static_cols.append('api_year') %}
  {% endif %}

  {# Use SQL Server-aware JSON extraction #}
  {% set rel_expr = jget('education_organization_reference:link:rel::string') %}
  {% set org_id_expr = jget('education_organization_reference:educationOrganizationId::string') %}

  case 
      when {{ rel_expr }} = 'School' 
      then null
      when {{ rel_expr }} = 'LocalEducationAgency'
      then {{ dbt_utils.generate_surrogate_key(static_cols + ['lower(' ~ org_id_expr ~ ')']) }}
  end as k_lea,
  case 
      when {{ rel_expr }} = 'School'
      then {{ dbt_utils.generate_surrogate_key(static_cols + ['lower(' ~ org_id_expr ~ ')']) }}
      when {{ rel_expr }} = 'LocalEducationAgency' 
      then null
  end as k_school
{%- endmacro %}