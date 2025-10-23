with stage_stu_programs as (
    select * from {{ ref('stg_ef3__student_title_i_part_a_program_associations') }}
),

flattened as (
    select
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,

        {{ extract_descriptor('value:serviceDescriptor::string') }} as service,
        {{ jget('value:primaryIndicator::boolean') }} as primary_indicator,
        {{ jget('value:serviceBeginDate::date') }} as service_begin_date,
        {{ jget('value:serviceEndDate::date') }} as service_end_date,

        -- edfi extensions
        {{ jget('value:_ext::string') }} as v_ext from stage_stu_programs
        {{ json_flatten('v_services') }}
)

select * from flattened
