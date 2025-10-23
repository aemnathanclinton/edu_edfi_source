with stage_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        ed_org_id,
        k_lea,
        k_school,
        -- Extract student characteristic directly from JSON
        substring(
            try_cast(json_value(char.value, '$.studentCharacteristicDescriptor') as nvarchar(max)), 
            charindex('#', try_cast(json_value(char.value, '$.studentCharacteristicDescriptor') as nvarchar(max))) + 1, 
            len(try_cast(json_value(char.value, '$.studentCharacteristicDescriptor') as nvarchar(max)))
        ) as student_characteristic,
        json_value(char.value, '$.designatedBy') as designated_by,
        -- For periods, use simplified approach - just get the first period if exists
        cast(json_value(json_query(char.value, '$.periods[0]'), '$.beginDate') as date) as begin_date,
        cast(json_value(json_query(char.value, '$.periods[0]'), '$.endDate') as date) as end_date
    from stage_stu_ed_org
        {{ json_flatten('v_student_characteristics', 'char', outer=True) }}
)
select * from flattened

