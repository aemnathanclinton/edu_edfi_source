with student_section as (
    {{ source_edfi3('student_section_associations') }}
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
        {{ jget("v:id::string") }}                                 as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:studentReference:studentUniqueId::string") }}   as student_unique_id,
        {{ jget("v:sectionReference:localCourseCode::string") }}   as local_course_code,
        {{ jget("v:sectionReference:schoolId::integer") }}         as school_id,
        {{ jget("v:sectionReference:schoolYear::integer") }}       as school_year,
        {{ jget("v:sectionReference:sectionIdentifier::string") }} as section_id,
        {{ jget("v:sectionReference:sessionName::string") }}       as session_name,
        {{ jget("v:beginDate::date") }}                            as begin_date,
        {{ jget("v:endDate::date") }}                              as end_date,
        {{ jget("v:homeroomIndicator::boolean") }}                 as is_homeroom,
        {{ jget("v:teacherStudentDataLinkExclusion::boolean") }}   as teacher_student_data_link_exclusion,
        -- descriptors
        {{ extract_descriptor('v:attemptStatusDescriptor::string') }} as attempt_status,
        {{ extract_descriptor('v:repeatIdentifierDescriptor::string') }} as repeat_identifier,
        -- references
        {{ jget("v:studentReference") }} as student_reference,
        {{ jget("v:sectionReference") }} as section_reference,

        -- lists
        {{ jget("v:programs::string") }} as v_programs,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from student_section
)
select * from renamed