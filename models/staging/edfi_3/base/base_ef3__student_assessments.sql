with student_assessments as (
    {{ source_edfi3('student_assessments') }}
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

        {{ jget("v:id::string") }}                                       as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:assessmentReference:assessmentIdentifier::string") }} as assessment_identifier,
        {{ jget("v:assessmentReference:namespace::string") }}            as namespace,
        {{ jget("v:schoolYearTypeReference:schoolYear::int") }}          as school_year,
        {{ jget("v:administrationDate::timestamp") }}     as administration_date,
        {{ jget("v:administrationEndDate::timestamp") }}  as administration_end_date,
        {{ jget("v:studentAssessmentIdentifier::string") }}              as student_assessment_identifier,
        {{ jget("v:studentReference:studentUniqueId::string") }}         as student_unique_id,
        {{ jget("v:eventDescription::string") }}                         as event_description,
        {{ jget("v:serialNumber::string") }}                             as serial_number,
        -- descriptors
        {{ extract_descriptor('v:administrationEnvironmentDescriptor::string') }} as administration_environment,
        {{ extract_descriptor('v:administrationLanguageDescriptor::string') }}    as administration_language,
        {{ extract_descriptor('v:eventCircumstanceDescriptor::string') }}         as event_circumstance,
        {{ extract_descriptor('v:platformTypeDescriptor::string') }}              as platform_type,
        {{ extract_descriptor('v:reasonNotTestedDescriptor::string') }}           as reason_not_tested,
        {{ extract_descriptor('v:retestIndicatorDescriptor::string') }}           as retest_indicator,
        {{ extract_descriptor('v:whenAssessedGradeLevelDescriptor::string') }}    as when_assessed_grade_level,
        -- references
        {{ jget("v:assessmentReference") }}         as assessment_reference,
        {{ jget("v:studentReference") }}            as student_reference,
        {{ jget("v:schoolYearTypeReference") }}     as school_year_type_reference,
        -- unflattened lists
        {{ jget("v:scoreResults::string") }}                as v_score_results,
        {{ jget("v:performanceLevels::string") }}           as v_performance_levels,
        {{ jget("v:items::string") }}                       as v_items,
        {{ jget("v:studentObjectiveAssessments::string") }} as v_student_objective_assessments,
        {{ jget("v:accommodations::string") }}              as v_accommodations,
        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from student_assessments
)
select * from renamed
