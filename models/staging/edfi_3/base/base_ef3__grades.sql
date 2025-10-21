with grades as (
    {{ source_edfi3('grades') }}
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
        -- section key
        {{ jget("v:studentSectionAssociationReference:beginDate::date") }}         as section_begin_date,
        {{ jget("v:studentSectionAssociationReference:localCourseCode::string") }} as local_course_code,
        {{ jget("v:studentSectionAssociationReference:schoolId::integer") }}       as school_id,
        {{ jget("v:studentSectionAssociationReference:schoolYear::integer") }}     as school_year,
        {{ jget("v:studentSectionAssociationReference:sectionId::string") }}       as section_id,
        {{ jget("v:studentSectionAssociationReference:sessionName::string") }}     as session_name,
        {{ jget("v:studentSectionAssociationReference:studentUniqueId::string") }} as student_unique_id,
        --grading period key
        {{ jget("v:gradingPeriodReference:schoolId::integer") }}       as grading_period_school_id,
        {{ jget("v:gradingPeriodReference:schoolYear::integer") }}     as grading_period_school_year,
        {{ jget("v:gradingPeriodReference:periodSequence::integer") }} as period_sequence,
        {{ extract_descriptor('v:gradingPeriodReference:gradingPeriodDescriptor::string') }} as grading_period_descriptor,
        -- grade data
        {{ jget("v:letterGradeEarned::string") }}   as letter_grade_earned,
        {{ jget("v:numericGradeEarned::float") }}   as numeric_grade_earned,
        {{ jget("v:diagnosticStatement::string") }} as diagnostic_statement,
        {{ extract_descriptor('v:gradeTypeDescriptor::string') }} as grade_type,
        {{ extract_descriptor('v:performanceBaseConversionDescriptor::string') }} as performance_base_conversion,
        -- embedded lists
        {{ jget("v:learningStandardGrades") }} as v_learning_standard_grades,
        -- references
        {{ jget("v:studentSectionAssociationReference") }} as student_section_association_reference,
        {{ jget("v:gradingPeriodReference") }}             as grading_period_reference,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from grades
)
select * from renamed
