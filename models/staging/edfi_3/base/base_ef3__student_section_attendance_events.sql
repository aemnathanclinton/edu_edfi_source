with stu_section_att as (
    {{ source_edfi3('student_section_attendance_events') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        __last_modified_timestamp as last_modified_timestamp,
        filename,
        file_row_number,
        __is_deleted as is_deleted,
        {{ jget("v:id::string") }}                                 as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:studentReference:studentUniqueId::string") }}   as student_unique_id,
        {{ jget("v:sectionReference:localCourseCode::string") }}   as local_course_code,
        {{ jget("v:sectionReference:schoolId::int") }}             as school_id,
        {{ jget("v:sectionReference:schoolYear::int") }}           as school_year,
        {{ jget("v:sectionReference:sectionIdentifier::string") }} as section_id,
        {{ jget("v:sectionReference:sessionName::string") }}       as session_name,
        {{ jget("v:eventDate::date") }}                            as attendance_event_date,
        {{ jget("v:attendanceEventReason::string") }}              as attendance_event_reason,
        {{ jget("v:eventDuration::float") }}                      as event_duration,
        {{ jget("v:sectionAttendanceDuration::float") }}           as section_attendance_duration,
        {{ jget("v:arrivalTime::string") }}                        as arrival_time, --todo: look at format here
        {{ jget("v:departureTime::string") }}                      as departure_time, --todo: look at format here
        -- descriptors
        {{ extract_descriptor('v:attendanceEventCategoryDescriptor::string', string_size=200) }} as attendance_event_category,
        {{ extract_descriptor('v:educationalEnvironmentDescriptor::string') }}  as educational_environment,
        -- references
        {{ jget("v:sectionReference") }} as section_reference,
        {{ jget("v:studentReference") }} as student_reference,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext

    from stu_section_att
)
select * from renamed
