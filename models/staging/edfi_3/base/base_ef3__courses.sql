with courses as (
    {{ source_edfi3('courses') }}
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
        {{ jget("v:id::string") }} as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }} as ed_org_id,
        {{ jget("v:educationOrganizationReference:link:rel::string") }}             as ed_org_type,
        {{ jget("v:courseCode::string") }}                      as course_code,
        {{ jget("v:courseTitle::string") }}                     as course_title,
        {{ jget("v:courseDescription::string") }}               as course_description,
        {{ jget("v:dateCourseAdopted::date") }}                 as date_course_adopted,
        {{ jget("v:highSchoolCourseRequirement::boolean") }}    as is_high_school_course_requirement,
        {{ jget("v:maxCompletionsForCredit::int") }}            as max_completions_for_credit,
        {{ jget("v:maximumAvailableCreditConversion::float") }} as maximum_available_credit_conversion,
        {{ jget("v:maximumAvailableCredits::float") }}          as maximum_available_credits,
        {{ jget("v:minimumAvailableCreditConversion::float") }} as minimum_available_credit_conversion,
        {{ jget("v:minimumAvailableCredits::float") }}          as minimum_available_credits,
        {{ jget("v:numberOfParts::int") }}                      as number_of_parts,
        {{ jget("v:timeRequiredForCompletion::int") }}          as time_required_for_completion,
        {{ extract_descriptor('v:academicSubjectDescriptor::string') }}            as academic_subject,
        {{ extract_descriptor('v:careerPathwayDescriptor::string') }}              as career_pathway,
        {{ extract_descriptor('v:courseDefinedByDescriptor::string') }}            as course_defined_by,
        {{ extract_descriptor('v:courseGPAApplicabilityDescriptor::string') }}     as gpa_applicability,
        {{ extract_descriptor('v:maximumAvailableCreditTypeDescriptor::string') }} as maximum_available_credit_type,
        {{ extract_descriptor('v:minimumAvailableCreditDescriptor::string') }}     as minimum_available_credit_type,
        -- references
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        -- unflattened lists
        {{ jget("v:identificationCodes") }}  as v_identification_codes,
        {{ jget("v:competencyLevels") }}     as v_competency_levels,
        {{ jget("v:learningObjectives") }}   as v_learning_objectives,
        {{ jget("v:learningStandards") }}    as v_learning_standards,
        {{ jget("v:levelCharacteristics") }} as v_level_characteristics,
        {{ jget("v:offeredGradeLevels") }}   as v_offered_grade_levels,
        {{ jget("v:academicSubjects") }}     as v_academic_subjects,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from courses
)
select * from renamed
