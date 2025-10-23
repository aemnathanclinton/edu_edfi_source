with stage_stu_ed_org as (
    select * from {{ ref('stg_ef3__student_education_organization_associations') }}
),
flattened as (
    select
        stage_stu_ed_org.tenant_code,
        stage_stu_ed_org.api_year,
        stage_stu_ed_org.k_student,
        stage_stu_ed_org.k_student_xyear,
        stage_stu_ed_org.ed_org_id,
        stage_stu_ed_org.k_lea,
        stage_stu_ed_org.k_school,
        {{ jget('ind.value:indicatorName::string') }}  as indicator_name,
        {{ jget('ind.value:indicator::string') }} as indicator_value,
        {{ jget('ind.value:indicatorGroup::string') }} as indicator_group,
        {{ jget('ind.value:designatedBy::string') }} as designated_by,
        {{ jget('timing.value:beginDate::date') }} as begin_date,
        {{ jget('timing.value:endDate::date') }} as end_date
    from stage_stu_ed_org
        {{ json_flatten('v_student_indicators', 'ind') }}
        {{ json_flatten(jget('ind.value:periods::array'), 'timing', outer=True) }}
)
select * from flattened
