with stu_sch_att as (
    {{ source_edfi3('student_school_attendance_events') }}
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
        {{ jget("v:id::string") }}                               as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:schoolReference:schoolId") }}                 as school_id,
        {{ jget("v:sessionReference:schoolYear") }}              as school_year,
        {{ jget("v:sessionReference:sessionName::string") }}     as session_name,
        {{ jget("v:studentReference:studentUniqueId::string") }} as student_unique_id,
        {{ jget("v:eventDate::date") }}                          as attendance_event_date,
        {{ jget("v:attendanceEventReason::string") }}            as attendance_event_reason,
        {{ jget("v:eventDuration::float") }}                    as event_duration,
        {{ jget("v:schoolAttendanceDuration::float") }}          as school_attendance_duration,
        {{ jget("v:arrivalTime::string") }}                      as arrival_time, --todo: look at format here
        {{ jget("v:departureTime::string") }}                    as departure_time, --todo: look at format here
        -- descriptors
        {{ extract_descriptor('v:attendanceEventCategoryDescriptor::string') }} as attendance_event_category,
        {{ extract_descriptor('v:educationalEnvironmentDescriptor::string') }}  as educational_environment,
        -- references
        {{ jget("v:schoolReference") }}  as school_reference,
        {{ jget("v:sessionReference") }} as session_reference,
        {{ jget("v:studentReference") }} as student_reference,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from stu_sch_att
)
select * from renamed
