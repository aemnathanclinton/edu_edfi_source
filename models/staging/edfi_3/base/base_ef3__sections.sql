with sections as (
    {{ source_edfi3('sections') }}
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
        {{ jget("v:id::string") }}                as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:sectionIdentifier::string") }} as section_id,
        {{ jget("v:sectionName::string") }}       as section_name,
        -- course offering key
        {{ jget("v:courseOfferingReference:schoolId::integer") }}             as school_id,
        {{ jget("v:courseOfferingReference:localCourseCode::string") }}       as local_course_code,
        {{ jget("v:courseOfferingReference:sessionName::string") }}           as session_name,
        {{ jget("v:courseOfferingReference:schoolYear::integer") }}           as school_year,
        -- location key
        {{ jget("v:locationReference:classroomIdentificationCode::string") }} as classroom_identification_code,
        {{ jget("v:locationReference:schoolId::integer") }}                   as classroom_location_school_id,
        -- location school key
		{{ jget("v:locationSchoolReference:schoolId::integer") }}             as school_location_school_id,
        -- values
        {{ jget("v:availableCreditConversion::float") }}  as available_credit_conversion,
        {{ jget("v:availableCredits::float") }}           as available_credits,
        {{ jget("v:sequenceOfCourse::integer") }}         as sequence_of_course,
        {{ jget("v:officialAttendancePeriod::boolean") }} as is_official_attendance_period,
        -- descriptors
        {{ extract_descriptor('v:availableCreditTypeDescriptor::string') }}    as available_credit_type,
        {{ extract_descriptor('v:educationalEnvironmentDescriptor::string') }} as educational_environment_type,
        {{ extract_descriptor('v:instructionLanguageDescriptor::string') }}    as instruction_language,
        {{ extract_descriptor('v:mediumOfInstructionDescriptor::string') }}    as medium_of_instruction,
        {{ extract_descriptor('v:populationServedDescriptor::string') }}       as population_served,
        {{ extract_descriptor('v:sectionTypeDescriptor::string') }}            as section_type,
        -- references
        {{ jget("v:courseOfferingReference") }} as course_offering_reference,
        {{ jget("v:locationReference") }}       as location_reference,
        {{ jget("v:locationSchoolReference") }} as location_school_reference,
        -- lists
        {{ jget("v:characteristics") }}            as v_section_characteristics,
        {{ jget("v:classPeriods") }}               as v_class_periods,
        {{ jget("v:courseLevelCharacteristics") }} as v_course_level_characteristics,
        {{ jget("v:offeredGradeLevels") }}         as v_offered_grade_levels,
        {{ jget("v:programs") }}                   as v_programs,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from sections
)
select * from renamed