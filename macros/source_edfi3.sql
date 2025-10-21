{#- Created an is_deleted flag using deletes resources -#}
{% macro source_edfi3(resource, join_deletes=True) -%}

    {% if join_deletes %}
        select
            api_data.*,
            {% if target.type == 'sqlserver' %}
                cast(case when try_cast(json_value(deletes_data.v, '$.id') as varchar(200)) is not null then 1 else 0 end as bit) as __is_deleted,
                coalesce(deletes_data.pull_timestamp, api_data.pull_timestamp) as __last_modified_timestamp
            {% else %}
                {{ edu_edfi_source.json_ci_get('deletes_data.v', 'id') }}::string is not null as is_deleted,
                coalesce(deletes_data.pull_timestamp, api_data.pull_timestamp) as last_modified_timestamp
            {% endif %}

        from {{ source('raw_edfi_3', resource) }} as api_data

            left join {{ source('raw_edfi_3', '_deletes') }} as deletes_data
            on (
                deletes_data.name = '{{ resource | lower }}'
                and api_data.tenant_code = deletes_data.tenant_code
                and api_data.api_year = deletes_data.api_year
                {% if target.type == 'sqlserver' %}
                    and json_value(api_data.v, '$.id') = replace(try_cast(json_value(deletes_data.v, '$.id') as varchar(200)), '-', '')
                {% else %}
                    and api_data.v:id::string = replace({{ edu_edfi_source.json_ci_get('deletes_data.v', 'id') }}::string, '-')
                {% endif %}
            )

    {% else %}
    select *, cast(0 as bit) as __is_deleted, pull_timestamp as __last_modified_timestamp
        from {{ source('raw_edfi_3', resource) }}

    {% endif %}

{%- endmacro %}
