with stage_student_assessments as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
stage_obj_assessments as (
    select * from {{ ref('stg_ef3__objective_assessments') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        k_student_assessment,
        k_assessment,
        k_student,
        k_student_xyear,
        student_assessment_identifier,
        assessment_identifier,
        namespace,
        academic_subject,
        school_year,
        administration_date,
        administration_end_date,
        event_description,
        administration_environment,
        administration_language,
        event_circumstance,
        platform_type,
        reason_not_tested,
        retest_indicator,
        when_assessed_grade_level,
        {{ jget('value:objectiveAssessmentReference:identificationCode::string') }} as objective_assessment_identification_code,
        {{ jget('value:objectiveAssessmentReference') }} as objective_assessment_reference,
        -- unflattened lists
        {{ jget('value:performanceLevels') }} as v_performance_levels,
        {{ jget('value:scoreResults') }} as v_score_results
    from stage_student_assessments
        {{ json_flatten('v_student_objective_assessments') }}
),
-- join to get subject from stg obj assess (if not null)
joined as (
    select
      flattened.tenant_code,
      flattened.api_year,
      flattened.pull_timestamp,
      flattened.last_modified_timestamp,
      flattened.k_student_assessment,
      flattened.k_assessment,
      flattened.k_student,
      flattened.k_student_xyear,
      flattened.student_assessment_identifier,
      flattened.assessment_identifier,
      flattened.namespace,
      flattened.school_year,
      flattened.administration_date,
      flattened.administration_end_date,
      flattened.event_description,
      flattened.administration_environment,
      flattened.administration_language,
      flattened.event_circumstance,
      flattened.platform_type,
      flattened.reason_not_tested,
      flattened.retest_indicator,
      flattened.when_assessed_grade_level,
      flattened.objective_assessment_identification_code,
      flattened.objective_assessment_reference,
      flattened.v_performance_levels,
      flattened.v_score_results,
      coalesce(stage_obj_assessments.academic_subject, flattened.academic_subject) as academic_subject
    from flattened
    join stage_obj_assessments
      on flattened.tenant_code = stage_obj_assessments.tenant_code
      and flattened.api_year = stage_obj_assessments.api_year
      and flattened.assessment_identifier = stage_obj_assessments.assessment_identifier
      and flattened.namespace = stage_obj_assessments.namespace
      and flattened.objective_assessment_identification_code = stage_obj_assessments.objective_assessment_identification_code
),
{# TEMPORARY Rename academic_subject --> obj_assess_academic_subject & overall --> academic_subject to allow the gen_skey() call to behave consistent with stg_ef3__objective_assessments #}
renamed1 as (
    select 
        joined.*
    from joined
),
keyed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(academic_subject)',
            'lower(assessment_identifier)',
            'lower(namespace)',
            'lower(objective_assessment_identification_code)',
            'lower(student_assessment_identifier)']
        ) }} as k_student_objective_assessment,
        {{ gen_skey('k_objective_assessment', extras = ['academic_subject']) }},
        k_student_assessment,
        k_assessment,
        k_student,
        k_student_xyear,
        student_assessment_identifier,
        assessment_identifier,
        namespace,
        academic_subject,
        objective_assessment_identification_code,
        objective_assessment_reference,
        school_year,
        administration_date,
        administration_end_date,
        event_description,
        administration_environment,
        administration_language,
        event_circumstance,
        platform_type,
        reason_not_tested,
        retest_indicator,
        when_assessed_grade_level,
        v_performance_levels,
        v_score_results
    from renamed1
),
{# Rename BACK for human-readability and to avoid breaking change to warehouse. #}
renamed2 as (
    select 
        keyed.*
    from keyed
),
-- todo: we already dedupe in student assessments so this is actually only necessary if we think there
    -- could be dupes in the objective assessments list
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='renamed2',
            partition_by='k_student_objective_assessment',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select *
from deduped
