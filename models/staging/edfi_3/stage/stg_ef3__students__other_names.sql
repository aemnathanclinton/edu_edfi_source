with students as (
    select * from {{ ref('stg_ef3__students') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        {{ extract_descriptor('value:otherNameTypeDescriptor::varchar') }} as other_name_type,
        {{ jget('value:personalTitlePrefix::varchar') }} as personal_title_prefix,
        {{ jget('value:firstName::varchar') }} as first_name,
        {{ jget('value:middleName::varchar') }} as middle_name,
        {{ jget('value:lastSurname::varchar') }} as last_surname,
        {{ jget('value:generationCodeSuffix::varchar') }} as generation_code_suffix from students
        {{ json_flatten('v_other_names') }}
)

select * from flattened
