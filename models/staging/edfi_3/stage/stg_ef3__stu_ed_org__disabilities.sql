with stg_stu_ed_org as (
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
        {{ extract_descriptor(jget('disab.value:disabilityDescriptor::string')) }} as disability_type,
        {{ extract_descriptor(jget('disab.value:disabilityDeterminationSourceTypeDescriptor::string')) }} as disability_source_type,
        {{ jget('disab.value:disabilityDiagnosis::string') }} as disability_diagnosis,
        {{ jget('disab.value:orderOfDisability::int') }} as order_of_disability,
        -- todo: perhaps these would better serve as wide booleans
        -- in which case we would not want to double-flatten, but leave nested
        -- for a downstream step
        {{ extract_descriptor(jget('desig.value:disabilityDesignationDescriptor::string')) }} as disability_designation 
    from stg_stu_ed_org
        {{ json_flatten('v_disabilities', 'disab') }}
        {{ json_flatten('json_value(disab.value, \'$.designations\')', 'desig') }}
)
select * from flattened
