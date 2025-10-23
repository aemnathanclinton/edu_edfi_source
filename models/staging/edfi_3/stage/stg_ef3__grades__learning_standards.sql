with stg_grades as (
    select * from {{ ref('stg_ef3__grades') }}
),
flattened as (
    select
        stg_grades.tenant_code,
        stg_grades.api_year,
        stg_grades.k_student,
        stg_grades.k_school,
        stg_grades.k_grading_period,
        stg_grades.k_course_section,
        stg_grades.grade_type,
        {{ gen_skey('k_learning_standard', alt_ref=jget('v_lsg.value:learningStandardReference')) }},
        {{ jget('v_lsg.value:learningStandardReference:learningStandardId::string') }} as learning_standard_id,
        {{ jget('v_lsg.value:letterGradeEarned::string') }} as learning_standard_letter_grade_earned,
        {{ jget('v_lsg.value:numericGradeEarned::string') }} as learning_standard_numeric_grade_earned,
        {{ extract_descriptor(jget('v_lsg.value:performanceBaseConversionDescriptor::string')) }} as performance_base_conversion_descriptor
    from stg_grades
        {{ json_flatten('v_learning_standard_grades', 'v_lsg') }}
)
select * from flattened
