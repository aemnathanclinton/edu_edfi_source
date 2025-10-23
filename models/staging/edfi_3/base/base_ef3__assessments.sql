with assessments as (
    {{ source_edfi3('assessments') }}
),
renamed as (
    select 
        tenant_code,
        api_year,
        pull_timestamp,
    __last_modified_timestamp as last_modified_timestamp,
        file_row_number,
        filename,
    __is_deleted as is_deleted,

        {{ jget("v:id::string") }}                   as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:assessmentIdentifier::string") }} as assessment_identifier,
        {{ jget("v:namespace::string") }}            as namespace,
        {{ jget("v:assessmentTitle::string") }}      as assessment_title,
        {{ jget("v:assessmentFamily::string") }}     as assessment_family,
        {{ jget("v:assessmentForm::string") }}       as assessment_form,
        {{ jget("v:assessmentVersion::string") }}    as assessment_version,
        {{ jget("v:maxRawScore::float") }}           as max_raw_score,
        {{ jget("v:nomenclature::string") }}         as nomenclature,
        {{ jget("v:adaptiveAssessment::boolean") }}  as is_adaptive_assessment,
        {{ jget("v:revisionDate::date") }}           as revision_date,
        {{ jget("v:period:beginDate::date") }}       as assessment_period_begin_date,
        {{ jget("v:period:endDate::date") }}         as assessment_period_end_date,
        {{ jget("v:contentStandard") }}              as content_standard,
        -- descriptors
        {{ extract_descriptor('v:assessmentCategoryDescriptor::string') }}      as assessment_category,
        {{ extract_descriptor('v:period:assessmentPeriodDescriptor::string') }} as assessment_period,
        cast(case
            when {{ json_array_size(jget("v:academicSubjects")) }} > 1 then 0
            else 1
        end as bit)                            as is_single_subject_identifier,
        --references
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        -- unflattened lists
        {{ jget("v:academicSubjects::string") }}    as v_academic_subjects,
        {{ jget("v:assessedGradeLevels::string") }} as v_assessed_grade_levels,
        {{ jget("v:performanceLevels::string") }}   as v_performance_levels,
        {{ jget("v:scores::string") }}              as v_scores,
        {{ jget("v:identificationCodes::string") }} as v_identification_codes,
        {{ jget("v:languages::string") }}           as v_languages,
        {{ jget("v:platformTypes::string") }}       as v_platform_types,
        {{ jget("v:programs::string") }}            as v_programs,
        {{ jget("v:sections::string") }}            as v_sections,
        {{ jget("v:authors::string") }}             as v_authors,
        -- unused
        {{ jget("v:contentStandard::string") }}     as v_content_standard,
        -- edfi extensions
        {{ jget("v:_ext::string") }}                as v_ext
    from assessments
)
select * from renamed
