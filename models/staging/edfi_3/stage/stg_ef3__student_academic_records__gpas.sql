with stg_academic_records as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student_academic_record,
        {{ extract_descriptor('value:gradePointAverageTypeDescriptor::string') }} as gpa_type,
        {{ jget('value:gradePointAverageValue::float') }} as gpa_value,
        {{ jget('value:isCumulative::boolean') }} as is_cumulative,
        {{ jget('value:maxGradePointAverageValue::float') }}  as max_gpa_value,
        -- edfi extensions
        {{ jget('value:_ext') }} as v_ext 
    from stg_academic_records
        {{ json_flatten('v_grade_point_averages') }}
),
-- pull out extensions from v_gpas.v_ext to their own columns
extended as (
    select
        flattened.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from flattened
)
select * from extended
